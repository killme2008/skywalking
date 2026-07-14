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
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.core.storage.model.ModelColumn;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBSearchableTagColumns.TagColumn;

/**
 * Caches the mapping from SkyWalking Model to GreptimeDB table name and gRPC write schema.
 * Thread-safe via ConcurrentHashMap.
 */
public class SchemaRegistry {
    private final ConcurrentMap<String, String> tableNames = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, WriteSchemaInfo> writeSchemas = new ConcurrentHashMap<>();
    private final GreptimeDBSearchableTagColumns tagColumns;

    public SchemaRegistry() {
        this(null);
    }

    public SchemaRegistry(final GreptimeDBSearchableTagColumns tagColumns) {
        this.tagColumns = tagColumns;
    }

    /**
     * Get the GreptimeDB table name for the given model. Cached after first resolution.
     */
    public String getTableName(final Model model) {
        return tableNames.computeIfAbsent(
            modelKey(model),
            k -> GreptimeDBConverter.resolveTableName(model)
        );
    }

    /**
     * Get the gRPC write schema info for the given model. Cached after first build.
     */
    public WriteSchemaInfo getWriteSchema(final Model model) {
        return writeSchemas.computeIfAbsent(
            modelKey(model),
            k -> buildWriteSchema(model)
        );
    }

    private String modelKey(final Model model) {
        return model.getName() + "_" + model.getDownsampling().getValue();
    }

    private WriteSchemaInfo buildWriteSchema(final Model model) {
        final String tableName = getTableName(model);
        final Set<String> pkColumns = GreptimeDBConverter.selectPrimaryKeyColumns(model)
                                                         .stream()
                                                         .collect(Collectors.toSet());
        final boolean expandTags = tagColumns != null && tagColumns.expandsTags(model);
        final List<TagColumn> searchableTags = expandTags
            ? tagColumns.resolve(model) : Collections.emptyList();

        final TableSchema.Builder builder = TableSchema.newBuilder(tableName);
        final List<String> columnNames = new ArrayList<>();
        final List<DataType> dataTypes = new ArrayList<>();

        // Synthetic id column (computed by StorageData.id().build(), not a @Column).
        // For non-timeSeries it is a PK (tag); for timeSeries it is a regular field.
        columnNames.add("id");
        dataTypes.add(DataType.String);
        if (pkColumns.contains("id")) {
            builder.addTag("id", DataType.String);
        } else {
            builder.addField("id", DataType.String);
        }

        for (final ModelColumn col : model.getColumns()) {
            final String colName = col.getColumnName().getStorageName();
            if (expandTags && GreptimeDBSearchableTagColumns.isTagsColumn(colName)) {
                // The JSON tags column is replaced by the per-key searchable tag columns below.
                continue;
            }
            final DataType dataType = GreptimeDBConverter.mapDataType(col);
            columnNames.add(colName);
            dataTypes.add(dataType);

            if (pkColumns.contains(colName)) {
                builder.addTag(colName, dataType);
            } else {
                builder.addField(colName, dataType);
            }
        }

        // Per-key searchable tag columns, matching the created table (PK tags are tags, the rest fields).
        for (final TagColumn tag : searchableTags) {
            columnNames.add(tag.getKey());
            dataTypes.add(DataType.String);
            if (tag.isPrimaryKey()) {
                builder.addTag(tag.getKey(), DataType.String);
            } else {
                builder.addField(tag.getKey(), DataType.String);
            }
        }

        // TIME INDEX column (always last)
        builder.addTimestamp("greptime_ts", DataType.TimestampMillisecond);
        columnNames.add("greptime_ts");
        dataTypes.add(DataType.TimestampMillisecond);

        return new WriteSchemaInfo(builder.build(), columnNames, dataTypes);
    }

    /**
     * Holds a gRPC TableSchema, the ordered column names, and their expected DataTypes
     * for building rows with defensive type coercion.
     */
    @Getter
    public static class WriteSchemaInfo {
        private final TableSchema tableSchema;
        private final List<String> columnNames;
        private final List<DataType> dataTypes;

        public WriteSchemaInfo(final TableSchema tableSchema,
                               final List<String> columnNames,
                               final List<DataType> dataTypes) {
            this.tableSchema = tableSchema;
            this.columnNames = columnNames;
            this.dataTypes = dataTypes;
        }
    }
}
