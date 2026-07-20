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

import io.greptime.models.DataType;
import io.greptime.models.TableSchema;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.core.storage.model.ModelColumn;
import org.apache.skywalking.oap.server.core.storage.model.SQLDatabaseModelExtension.AdditionalTable;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBIndexPolicy.IndexType;

/**
 * The single schema contract used by DDL generation, gRPC row encoding and boot-time validation.
 */
@Getter
public final class GreptimeDBTableSchema {
    public static final String ID = "id";
    public static final String TIMESTAMP = "greptime_ts";

    private final String tableName;
    private final List<Column> columns;
    private final List<String> primaryKeys;
    private final Map<String, String> options;
    private final String listColumn;

    private GreptimeDBTableSchema(final String tableName,
                                  final List<Column> columns,
                                  final List<String> primaryKeys,
                                  final Map<String, String> options,
                                  final String listColumn) {
        this.tableName = tableName;
        this.columns = Collections.unmodifiableList(new ArrayList<>(columns));
        this.primaryKeys = Collections.unmodifiableList(new ArrayList<>(primaryKeys));
        this.options = Collections.unmodifiableMap(new LinkedHashMap<>(options));
        this.listColumn = listColumn;
    }

    public static GreptimeDBTableSchema forModel(final Model model,
                                                 final GreptimeDBStorageConfig config,
                                                 final GreptimeDBIndexPolicy indexPolicy) {
        final List<String> primaryKeys = GreptimeDBConverter.selectPrimaryKeyColumns(model);
        final Set<String> primaryKeySet = new HashSet<>(primaryKeys);
        final Set<String> excluded = model.getSqlDBModelExtension().getExcludeColumns().stream()
            .map(column -> column.getColumnName().getStorageName())
            .collect(Collectors.toSet());
        final List<Column> columns = new ArrayList<>();

        columns.add(new Column(ID, "STRING", DataType.String,
            primaryKeySet.contains(ID) ? SemanticType.TAG : SemanticType.FIELD,
            IndexType.NONE, null));

        for (final ModelColumn modelColumn : model.getColumns()) {
            final String columnName = modelColumn.getColumnName().getStorageName();
            if (List.class.isAssignableFrom(modelColumn.getType()) && !excluded.contains(columnName)) {
                throw new IllegalArgumentException(
                    "List column " + model.getName() + '.' + columnName
                        + " must declare SQLDatabase.AdditionalEntity");
            }
            if (excluded.contains(columnName)) {
                continue;
            }
            final boolean primaryKey = primaryKeySet.contains(columnName);
            columns.add(new Column(
                columnName,
                GreptimeDBConverter.mapToSqlType(modelColumn),
                GreptimeDBConverter.mapDataType(modelColumn),
                primaryKey ? SemanticType.TAG : SemanticType.FIELD,
                primaryKey ? IndexType.NONE : indexPolicy.resolve(model, modelColumn),
                null
            ));
        }

        columns.add(new Column(
            TIMESTAMP,
            "TIMESTAMP",
            DataType.TimestampMillisecond,
            SemanticType.TIMESTAMP,
            IndexType.NONE,
            model.isTimeSeries() ? null : "CURRENT_TIMESTAMP()"
        ));

        final Map<String, String> options = new LinkedHashMap<>();
        if (model.isRecord() && model.isTimeSeries()) {
            options.put("append_mode", "true");
        } else {
            options.put("merge_mode", "last_row");
        }
        options.put("ttl", resolveTTL(model, config));
        return new GreptimeDBTableSchema(
            GreptimeDBConverter.resolveTableName(model), columns, primaryKeys, options, null);
    }

    public static GreptimeDBTableSchema forAdditionalTable(final AdditionalTable additionalTable,
                                                           final GreptimeDBStorageConfig config) {
        final List<Column> columns = new ArrayList<>();
        columns.add(new Column(ID, "STRING", DataType.String,
            SemanticType.FIELD, IndexType.NONE, null));

        String listColumn = null;
        for (final ModelColumn modelColumn : additionalTable.getColumns()) {
            final String columnName = modelColumn.getColumnName().getStorageName();
            final boolean list = List.class.isAssignableFrom(modelColumn.getType());
            if (!list) {
                continue;
            }
            listColumn = columnName;
            columns.add(new Column(
                columnName,
                "STRING",
                DataType.String,
                SemanticType.FIELD,
                IndexType.SKIPPING,
                null
            ));
        }
        if (listColumn == null) {
            throw new IllegalArgumentException(
                "Additional table " + additionalTable.getName() + " has no List column");
        }
        columns.add(new Column(TIMESTAMP, "TIMESTAMP", DataType.TimestampMillisecond,
            SemanticType.TIMESTAMP, IndexType.NONE, null));

        final Map<String, String> options = new LinkedHashMap<>();
        options.put("append_mode", "true");
        options.put("ttl", config.getRecordsTTL());
        return new GreptimeDBTableSchema(
            GreptimeDBConverter.normalizeTableName(additionalTable.getName()),
            columns, Collections.emptyList(), options, listColumn);
    }

    private static String resolveTTL(final Model model, final GreptimeDBStorageConfig config) {
        if (!model.isTimeSeries()) {
            return "forever";
        }
        return model.isMetric() ? config.getMetricsTTL() : config.getRecordsTTL();
    }

    public String buildCreateTableDDL() {
        final StringBuilder ddl = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
            .append(tableName).append(" (\n");
        final List<String> definitions = new ArrayList<>();
        for (final Column column : columns) {
            final StringBuilder definition = new StringBuilder("  ")
                .append(GreptimeDBConverter.quoteColumn(column.name))
                .append(' ').append(column.sqlType);
            if (column.defaultExpression != null) {
                definition.append(" DEFAULT ").append(column.defaultExpression);
            }
            if (column.semanticType == SemanticType.TIMESTAMP) {
                definition.append(" TIME INDEX");
            } else {
                definition.append(column.indexType.ddl());
            }
            definitions.add(definition.toString());
        }
        if (!primaryKeys.isEmpty()) {
            definitions.add("  PRIMARY KEY (" + primaryKeys.stream()
                .map(GreptimeDBConverter::quoteColumn)
                .collect(Collectors.joining(", ")) + ")");
        }
        ddl.append(String.join(",\n", definitions)).append("\n)");
        if (!options.isEmpty()) {
            ddl.append(" WITH (")
                .append(options.entrySet().stream()
                    .map(entry -> "'" + entry.getKey() + "' = '" + entry.getValue() + "'")
                    .collect(Collectors.joining(", ")))
                .append(')');
        }
        return ddl.toString();
    }

    public TableSchema buildWriteSchema() {
        final TableSchema.Builder builder = TableSchema.newBuilder(tableName);
        for (final Column column : columns) {
            switch (column.semanticType) {
                case TAG:
                    builder.addTag(column.name, column.dataType);
                    break;
                case TIMESTAMP:
                    builder.addTimestamp(column.name, column.dataType);
                    break;
                case FIELD:
                default:
                    builder.addField(column.name, column.dataType);
                    break;
            }
        }
        return builder.build();
    }

    public String fingerprint() {
        return tableName + ':' + columns.stream()
            .map(column -> column.name + '/' + column.sqlType + '/' + column.dataType + '/'
                + column.semanticType + '/' + column.indexType + '/' + column.defaultExpression)
            .collect(Collectors.joining(","))
            + ":pk=" + primaryKeys + ":options=" + options + ":list=" + listColumn;
    }

    @Getter
    public static final class Column {
        private final String name;
        private final String sqlType;
        private final DataType dataType;
        private final SemanticType semanticType;
        private final IndexType indexType;
        private final String defaultExpression;

        private Column(final String name,
                       final String sqlType,
                       final DataType dataType,
                       final SemanticType semanticType,
                       final IndexType indexType,
                       final String defaultExpression) {
            this.name = name;
            this.sqlType = sqlType;
            this.dataType = dataType;
            this.semanticType = semanticType;
            this.indexType = indexType;
            this.defaultExpression = defaultExpression;
        }
    }

    public enum SemanticType {
        TAG,
        FIELD,
        TIMESTAMP
    }
}
