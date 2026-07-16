/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.skywalking.oap.server.storage.plugin.greptimedb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.oap.server.core.storage.StorageException;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.core.storage.model.ModelInstaller;
import org.apache.skywalking.oap.server.core.storage.model.StorageManipulationOpt;
import org.apache.skywalking.oap.server.library.module.ModuleManager;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBIndexPolicy.IndexType;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBTableSchema.Column;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBTableSchema.SemanticType;

@Slf4j
public class GreptimeDBTableInstaller extends ModelInstaller {
    private static final Pattern TTL_TOKEN = Pattern.compile(
        "(\\d+)\\s*(milliseconds?|ms|seconds?|secs?|s|minutes?|mins?|m|hours?|hrs?|h|days?|d|weeks?|w)",
        Pattern.CASE_INSENSITIVE);

    private final GreptimeDBStorageConfig config;
    private final SchemaRegistry schemaRegistry;

    public GreptimeDBTableInstaller(final GreptimeDBStorageClient client,
                                    final ModuleManager moduleManager,
                                    final GreptimeDBStorageConfig config,
                                    final SchemaRegistry schemaRegistry) {
        super(client, moduleManager);
        this.config = config;
        this.schemaRegistry = schemaRegistry;
    }

    @Override
    public InstallInfo isExists(final Model model, final StorageManipulationOpt opt) throws StorageException {
        final GreptimeDBInstallInfo info = new GreptimeDBInstallInfo(model);
        try (Connection connection = ((GreptimeDBStorageClient) client).getConnection()) {
            final List<GreptimeDBTableSchema> schemas = schemasFor(model);
            final Set<String> existingTables = loadExistingTables(connection);
            boolean allExist = true;
            for (final GreptimeDBTableSchema schema : schemas) {
                if (!existingTables.contains(schema.getTableName())) {
                    allExist = false;
                    opt.recordOutcome("table", schema.getTableName(),
                        StorageManipulationOpt.Outcome.MISSING, "table not found");
                    continue;
                }
                final SchemaDiff diff = diff(connection, schema);
                if (diff.isEmpty()) {
                    opt.recordOutcome("table", schema.getTableName(),
                        StorageManipulationOpt.Outcome.EXISTING_MATCHED, null);
                } else {
                    opt.recordOutcome("table", schema.getTableName(),
                        StorageManipulationOpt.Outcome.SKIPPED_SHAPE_MISMATCH, diff.describe());
                    throw new StorageException(
                        "GreptimeDB table " + schema.getTableName()
                            + " does not match the declared schema. Drop and recreate it. diff: "
                            + diff.describe());
                }
            }
            info.setAllExist(allExist);
            return info;
        } catch (SQLException e) {
            throw new StorageException("Failed to inspect GreptimeDB schema for " + model.getName(), e);
        }
    }

    @Override
    public void createTable(final Model model) throws StorageException {
        for (final GreptimeDBTableSchema schema : schemasFor(model)) {
            final String ddl = schema.buildCreateTableDDL();
            log.info("Creating GreptimeDB table: {}", schema.getTableName());
            log.debug("DDL: {}", ddl);
            try {
                ((GreptimeDBStorageClient) client).executeDDL(ddl);
            } catch (SQLException e) {
                throw new StorageException("Failed to create GreptimeDB table " + schema.getTableName(), e);
            }
        }
    }

    String buildCreateTableDDL(final Model model) {
        return schemaRegistry.getWriteSchema(model).getSchema().buildCreateTableDDL();
    }

    List<String> selectPrimaryKeyColumns(final Model model) {
        return GreptimeDBConverter.selectPrimaryKeyColumns(model);
    }

    private List<GreptimeDBTableSchema> schemasFor(final Model model) {
        return schemaRegistry.getWriteSchemas(model).stream()
            .map(SchemaRegistry.WriteSchemaInfo::getSchema)
            .collect(Collectors.toList());
    }

    private Set<String> loadExistingTables(final Connection connection) throws SQLException {
        final Set<String> tables = new HashSet<>();
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = ?")) {
            statement.setString(1, config.getDatabase());
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    tables.add(resultSet.getString(1));
                }
            }
        }
        return tables;
    }

    private SchemaDiff diff(final Connection connection,
                            final GreptimeDBTableSchema desired) throws SQLException {
        final ActualSchema actual = inspect(connection, desired.getTableName());
        final SchemaDiff diff = new SchemaDiff();
        final Map<String, Column> desiredColumns = desired.getColumns().stream()
            .collect(Collectors.toMap(Column::getName, column -> column, (left, right) -> left, LinkedHashMap::new));

        for (final Column column : desired.getColumns()) {
            final ActualColumn existing = actual.columns.get(column.getName());
            if (existing == null) {
                diff.mismatches.add("missing column " + column.getName());
                continue;
            }
            if (!normalizeType(existing.dataType).equals(normalizeType(column.getDataType().name()))) {
                diff.mismatches.add("type " + column.getName() + " expected="
                    + column.getDataType() + " actual=" + existing.dataType);
            }
            if (existing.semanticType != column.getSemanticType()) {
                diff.mismatches.add("semantic " + column.getName() + " expected="
                    + column.getSemanticType() + " actual=" + existing.semanticType);
            }
        }
        actual.columns.keySet().stream()
            .filter(column -> !desiredColumns.containsKey(column))
            .forEach(column -> diff.mismatches.add("unexpected column " + column));

        if (!desired.getPrimaryKeys().equals(actual.primaryKeys)) {
            diff.mismatches.add("primary key expected=" + desired.getPrimaryKeys()
                + " actual=" + actual.primaryKeys);
        }

        for (final Column column : desired.getColumns()) {
            if (!actual.columns.containsKey(column.getName())) {
                continue;
            }
            final Set<IndexType> expected = column.getIndexType() == IndexType.NONE
                ? java.util.Collections.emptySet()
                : java.util.Collections.singleton(column.getIndexType());
            final Set<IndexType> existing = actual.indexes.getOrDefault(
                column.getName(), java.util.Collections.emptySet());
            if (!existing.equals(expected)) {
                diff.mismatches.add("indexes " + column.getName() + " expected="
                    + expected + " actual=" + existing);
            }
        }
        actual.indexes.keySet().stream()
            .filter(column -> !desiredColumns.containsKey(column))
            .forEach(column -> diff.mismatches.add(
                "indexes on unexpected column " + column + " actual=" + actual.indexes.get(column)));

        final Map<String, String> options = actual.options;
        final boolean appendMode = Boolean.parseBoolean(options.getOrDefault("append_mode", "false"));
        final boolean desiredAppendMode = Boolean.parseBoolean(
            desired.getOptions().getOrDefault("append_mode", "false"));
        if (desiredAppendMode != appendMode) {
            diff.mismatches.add("append_mode expected=" + desiredAppendMode + " actual=" + appendMode);
        }
        final String desiredMergeMode = desired.getOptions().getOrDefault("merge_mode", "last_row");
        final String actualMergeMode = options.getOrDefault("merge_mode", "last_row");
        if (!desiredAppendMode && !desiredMergeMode.equals(actualMergeMode)) {
            diff.mismatches.add("merge_mode expected=" + desiredMergeMode
                + " actual=" + actualMergeMode);
        }
        final String actualTTL = options.get("ttl");
        final String desiredTTL = desired.getOptions().get("ttl");
        if (!sameTTL(desiredTTL, actualTTL)) {
            diff.mismatches.add("ttl expected=" + desiredTTL + " actual=" + actualTTL);
        }
        return diff;
    }

    private ActualSchema inspect(final Connection connection, final String tableName) throws SQLException {
        final ActualSchema schema = new ActualSchema();
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT column_name, greptime_data_type, semantic_type "
                    + "FROM information_schema.columns WHERE table_schema = ? AND table_name = ? "
                    + "ORDER BY ordinal_position")) {
            statement.setString(1, config.getDatabase());
            statement.setString(2, tableName);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    schema.columns.put(resultSet.getString(1), new ActualColumn(
                        resultSet.getString(2), semanticType(resultSet.getString(3))));
                }
            }
        }
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT column_name, index_type, seq_in_index FROM information_schema.statistics "
                    + "WHERE table_schema = ? AND table_name = ? ORDER BY index_name, seq_in_index")) {
            statement.setString(1, config.getDatabase());
            statement.setString(2, tableName);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    final String column = resultSet.getString(1);
                    final String type = resultSet.getString(2).toUpperCase(Locale.ROOT);
                    if ("PRIMARY".equals(type)) {
                        schema.primaryKeys.add(column);
                    } else if ("INVERTED".equals(type)) {
                        schema.indexes.computeIfAbsent(column, ignored -> new HashSet<>())
                            .add(IndexType.INVERTED);
                    } else if ("SKIPPING".equals(type)) {
                        schema.indexes.computeIfAbsent(column, ignored -> new HashSet<>())
                            .add(IndexType.SKIPPING);
                    } else if ("FULLTEXT".equals(type)) {
                        schema.indexes.computeIfAbsent(column, ignored -> new HashSet<>())
                            .add(IndexType.FULLTEXT);
                    }
                }
            }
        }
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT create_options FROM information_schema.tables "
                    + "WHERE table_schema = ? AND table_name = ?")) {
            statement.setString(1, config.getDatabase());
            statement.setString(2, tableName);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    schema.options.putAll(parseOptions(resultSet.getString(1)));
                }
            }
        }
        return schema;
    }

    private static String normalizeType(final String type) {
        return type == null ? "" : type.replace("_", "").toLowerCase(Locale.ROOT);
    }

    private static SemanticType semanticType(final String value) {
        if ("TAG".equalsIgnoreCase(value) || "PRIMARY KEY".equalsIgnoreCase(value)) {
            return SemanticType.TAG;
        }
        if ("TIMESTAMP".equalsIgnoreCase(value) || "TIME INDEX".equalsIgnoreCase(value)) {
            return SemanticType.TIMESTAMP;
        }
        return SemanticType.FIELD;
    }

    private static Map<String, String> parseOptions(final String value) {
        final Map<String, String> options = new HashMap<>();
        if (value == null || value.trim().isEmpty()) {
            return options;
        }
        for (final String token : value.trim().split("\\s+")) {
            final int separator = token.indexOf('=');
            if (separator > 0) {
                options.put(token.substring(0, separator), token.substring(separator + 1));
            }
        }
        return options;
    }

    private static boolean sameTTL(final String left, final String right) {
        if (left == null || right == null) {
            return left == null && right == null;
        }
        if (left.equalsIgnoreCase(right)) {
            return true;
        }
        return ttlMillis(left) == ttlMillis(right);
    }

    private static long ttlMillis(final String value) {
        if ("forever".equalsIgnoreCase(value) || "0".equals(value) || "0d".equalsIgnoreCase(value)) {
            return Long.MAX_VALUE;
        }
        final Matcher matcher = TTL_TOKEN.matcher(value);
        long total = 0;
        int end = 0;
        while (matcher.find()) {
            if (!value.substring(end, matcher.start()).trim().isEmpty()) {
                return Long.MIN_VALUE;
            }
            final long amount = Long.parseLong(matcher.group(1));
            final String unit = matcher.group(2).toLowerCase(Locale.ROOT);
            if (unit.startsWith("w")) {
                total += amount * 7 * 24 * 60 * 60 * 1000;
            } else if (unit.startsWith("d")) {
                total += amount * 24 * 60 * 60 * 1000;
            } else if (unit.startsWith("h")) {
                total += amount * 60 * 60 * 1000;
            } else if (unit.startsWith("m") && !unit.startsWith("ms") && !unit.startsWith("millisecond")) {
                total += amount * 60 * 1000;
            } else if (unit.startsWith("s")) {
                total += amount * 1000;
            } else {
                total += amount;
            }
            end = matcher.end();
        }
        return end == value.length() ? total : Long.MIN_VALUE;
    }

    private static final class ActualSchema {
        private final Map<String, ActualColumn> columns = new LinkedHashMap<>();
        private final List<String> primaryKeys = new ArrayList<>();
        private final Map<String, Set<IndexType>> indexes = new HashMap<>();
        private final Map<String, String> options = new HashMap<>();
    }

    private static final class ActualColumn {
        private final String dataType;
        private final SemanticType semanticType;

        private ActualColumn(final String dataType, final SemanticType semanticType) {
            this.dataType = dataType;
            this.semanticType = semanticType;
        }
    }

    private static final class SchemaDiff {
        private final List<String> mismatches = new ArrayList<>();

        private boolean isEmpty() {
            return mismatches.isEmpty();
        }

        private String describe() {
            return String.join("; ", mismatches);
        }
    }

    private static class GreptimeDBInstallInfo extends InstallInfo {
        private GreptimeDBInstallInfo(final Model model) {
            super(model);
        }

        @Override
        public String buildInstallInfoMsg() {
            return "GreptimeDB table [" + getModelName() + "] "
                + (isAllExist() ? "exists" : "not found");
        }
    }
}
