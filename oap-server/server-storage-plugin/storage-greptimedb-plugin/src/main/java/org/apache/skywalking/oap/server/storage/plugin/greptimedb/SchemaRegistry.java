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
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.core.storage.model.ModelColumn;

/**
 * Caches the mapping from SkyWalking Model to GreptimeDB table name and gRPC write schema.
 * Thread-safe via ConcurrentHashMap.
 */
public class SchemaRegistry {
    private final ConcurrentMap<String, String> tableNames = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, WriteSchemaInfo> writeSchemas = new ConcurrentHashMap<>();

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

        final TableSchema.Builder builder = TableSchema.newBuilder(tableName);
        final List<String> columnNames = new ArrayList<>();

        // Synthetic id column (computed by StorageData.id().build(), not a @Column).
        // For non-timeSeries it is a PK (tag); for timeSeries it is a regular field.
        columnNames.add("id");
        if (pkColumns.contains("id")) {
            builder.addTag("id", DataType.String);
        } else {
            builder.addField("id", DataType.String);
        }

        for (final ModelColumn col : model.getColumns()) {
            final String colName = col.getColumnName().getStorageName();
            final DataType dataType = GreptimeDBConverter.mapDataType(col);
            columnNames.add(colName);

            if (pkColumns.contains(colName)) {
                builder.addTag(colName, dataType);
            } else {
                builder.addField(colName, dataType);
            }
        }

        // TIME INDEX column (always last)
        builder.addTimestamp("greptime_ts", DataType.TimestampMillisecond);
        columnNames.add("greptime_ts");

        return new WriteSchemaInfo(builder.build(), columnNames);
    }

    /**
     * Holds a gRPC TableSchema and the ordered column names for building rows.
     */
    @Getter
    public static class WriteSchemaInfo {
        private final TableSchema tableSchema;
        private final List<String> columnNames;

        public WriteSchemaInfo(final TableSchema tableSchema, final List<String> columnNames) {
            this.tableSchema = tableSchema;
            this.columnNames = columnNames;
        }
    }
}
