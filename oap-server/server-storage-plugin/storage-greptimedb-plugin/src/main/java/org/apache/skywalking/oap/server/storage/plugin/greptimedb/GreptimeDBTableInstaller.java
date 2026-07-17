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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
    private static final String TABLES_SQL =
        "SELECT table_name, create_options FROM information_schema.tables WHERE table_schema = ?";
    private static final String COLUMNS_SQL =
        "SELECT table_name, column_name, greptime_data_type, semantic_type "
            + "FROM information_schema.columns WHERE table_schema = ? "
            + "ORDER BY table_name, ordinal_position";
    private static final String INDEXES_SQL =
        "SELECT table_name, column_name, index_type, seq_in_index "
            + "FROM information_schema.statistics WHERE table_schema = ? "
            + "ORDER BY table_name, index_name, seq_in_index";
    private static final String LEGACY_INDEXES_SQL =
        "SELECT table_name, column_name, constraint_name, ordinal_position "
            + "FROM information_schema.key_column_usage WHERE table_schema = ? "
            + "ORDER BY table_name, ordinal_position";

    private final GreptimeDBStorageConfig config;
    private final SchemaRegistry schemaRegistry;
    private volatile SchemaSnapshot schemaSnapshot;

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
        final SchemaSnapshot snapshot = schemaSnapshot();
        boolean allExist = true;
        for (final GreptimeDBTableSchema desired : schemasFor(model)) {
            final ActualSchema actual = snapshot.tables.get(desired.getTableName());
            if (actual == null) {
                allExist = false;
                opt.recordOutcome("table", desired.getTableName(),
                    StorageManipulationOpt.Outcome.MISSING, "table not found");
                continue;
            }

            final SchemaDiff diff = diff(desired, actual);
            if (!diff.isEmpty()) {
                opt.recordOutcome("table", desired.getTableName(),
                    StorageManipulationOpt.Outcome.SKIPPED_SHAPE_MISMATCH, diff.describe());
                throw new StorageException(
                    "GreptimeDB table " + desired.getTableName()
                        + " does not match the declared schema. Drop and recreate it. diff: "
                        + diff.describe());
            }
            opt.recordOutcome("table", desired.getTableName(),
                StorageManipulationOpt.Outcome.EXISTING_MATCHED, null);
        }
        info.setAllExist(allExist);
        return info;
    }

    @Override
    public void createTable(final Model model) throws StorageException {
        for (final GreptimeDBTableSchema schema : schemasFor(model)) {
            final String ddl = schema.buildCreateTableDDL();
            log.info("Creating GreptimeDB table: {}", schema.getTableName());
            log.debug("DDL: {}", ddl);
            try {
                ((GreptimeDBStorageClient) client).executeDDL(ddl);
                final SchemaSnapshot snapshot = schemaSnapshot;
                if (snapshot != null) {
                    snapshot.tables.put(schema.getTableName(), ActualSchema.from(schema));
                }
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

    private SchemaSnapshot schemaSnapshot() throws StorageException {
        SchemaSnapshot snapshot = schemaSnapshot;
        if (snapshot != null) {
            return snapshot;
        }
        synchronized (this) {
            snapshot = schemaSnapshot;
            if (snapshot != null) {
                return snapshot;
            }
            try (Connection connection = ((GreptimeDBStorageClient) client).getConnection()) {
                snapshot = loadSchemaSnapshot(connection);
            } catch (SQLException e) {
                throw new StorageException("Failed to inspect GreptimeDB schemas", e);
            }
            schemaSnapshot = snapshot;
            return snapshot;
        }
    }

    private SchemaSnapshot loadSchemaSnapshot(final Connection connection) throws SQLException {
        final SchemaSnapshot snapshot = new SchemaSnapshot();
        try (PreparedStatement statement = connection.prepareStatement(TABLES_SQL)) {
            statement.setString(1, config.getDatabase());
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    final ActualSchema schema = new ActualSchema();
                    schema.options.putAll(parseOptions(resultSet.getString(2)));
                    snapshot.tables.put(resultSet.getString(1), schema);
                }
            }
        }

        try (PreparedStatement statement = connection.prepareStatement(COLUMNS_SQL)) {
            statement.setString(1, config.getDatabase());
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    final ActualSchema schema = snapshot.tables.computeIfAbsent(
                        resultSet.getString(1), ignored -> new ActualSchema());
                    schema.columns.put(resultSet.getString(2), new ActualColumn(
                        resultSet.getString(3), semanticType(resultSet.getString(4))));
                }
            }
        }

        loadIndexes(connection, snapshot);
        return snapshot;
    }

    private void loadIndexes(final Connection connection, final SchemaSnapshot snapshot) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(INDEXES_SQL)) {
            statement.setString(1, config.getDatabase());
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    addIndex(snapshot.tables.computeIfAbsent(
                        resultSet.getString(1), ignored -> new ActualSchema()),
                        resultSet.getString(2), resultSet.getString(3));
                }
            }
        } catch (SQLException e) {
            if (!isMissingStatisticsView(e)) {
                throw e;
            }
            log.info("GreptimeDB does not expose information_schema.statistics; "
                + "falling back to information_schema.key_column_usage");
            loadLegacyIndexes(connection, snapshot);
        }
    }

    private void loadLegacyIndexes(final Connection connection,
                                   final SchemaSnapshot snapshot) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(LEGACY_INDEXES_SQL)) {
            statement.setString(1, config.getDatabase());
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    addIndex(snapshot.tables.computeIfAbsent(
                        resultSet.getString(1), ignored -> new ActualSchema()),
                        resultSet.getString(2), resultSet.getString(3));
                }
            }
        }
    }

    private static void addIndex(final ActualSchema schema,
                                 final String column,
                                 final String rawType) {
        for (final String token : rawType.toUpperCase(Locale.ROOT).split(",")) {
            final String type = token.trim().replace(" INDEX", "");
            if ("PRIMARY".equals(type)) {
                if (!schema.primaryKeys.contains(column)) {
                    schema.primaryKeys.add(column);
                }
                continue;
            }
            final IndexType indexType = indexType(type);
            if (indexType != null) {
                schema.indexes.computeIfAbsent(column, ignored -> new HashSet<>()).add(indexType);
            }
        }
    }

    private static boolean isMissingStatisticsView(final SQLException exception) {
        Throwable cause = exception;
        while (cause != null) {
            final String message = cause.getMessage();
            if (message != null) {
                final String normalized = message.toLowerCase(Locale.ROOT);
                if (normalized.contains("information_schema.statistics")
                    && (normalized.contains("table not found")
                    || normalized.contains("tablenotfound")
                    || normalized.contains("doesn't exist")
                    || normalized.contains("unknown table"))) {
                    return true;
                }
            }
            cause = cause.getCause();
        }
        return false;
    }

    private SchemaDiff diff(final GreptimeDBTableSchema desired, final ActualSchema actual) {
        final SchemaDiff diff = new SchemaDiff();
        final Map<String, Column> desiredColumns = desired.getColumns().stream()
            .collect(Collectors.toMap(Column::getName, column -> column,
                (left, right) -> left, LinkedHashMap::new));

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
                ? Collections.emptySet() : Collections.singleton(column.getIndexType());
            final Set<IndexType> existing = actual.indexes.getOrDefault(
                column.getName(), Collections.emptySet());
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

    private static IndexType indexType(final String type) {
        if ("INVERTED".equals(type)) {
            return IndexType.INVERTED;
        }
        if ("SKIPPING".equals(type)) {
            return IndexType.SKIPPING;
        }
        if ("FULLTEXT".equals(type)) {
            return IndexType.FULLTEXT;
        }
        return null;
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
            } else if (unit.startsWith("m")
                && !unit.startsWith("ms") && !unit.startsWith("millisecond")) {
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

    private static final class SchemaSnapshot {
        private final Map<String, ActualSchema> tables = new ConcurrentHashMap<>();
    }

    private static final class ActualSchema {
        private final Map<String, ActualColumn> columns = new LinkedHashMap<>();
        private final List<String> primaryKeys = new ArrayList<>();
        private final Map<String, Set<IndexType>> indexes = new HashMap<>();
        private final Map<String, String> options = new HashMap<>();

        private static ActualSchema from(final GreptimeDBTableSchema desired) {
            final ActualSchema actual = new ActualSchema();
            for (final Column column : desired.getColumns()) {
                actual.columns.put(column.getName(), new ActualColumn(
                    column.getDataType().name(), column.getSemanticType()));
                if (column.getIndexType() != IndexType.NONE) {
                    actual.indexes.put(column.getName(), Collections.singleton(column.getIndexType()));
                }
            }
            actual.primaryKeys.addAll(desired.getPrimaryKeys());
            actual.options.putAll(desired.getOptions());
            return actual;
        }
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
