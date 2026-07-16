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
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.core.storage.model.SQLDatabaseModelExtension.AdditionalTable;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBTableSchema.Column;

/**
 * Caches the schema contract used for both DDL and gRPC writes.
 */
public class SchemaRegistry {
    private final ConcurrentMap<String, List<WriteSchemaInfo>> schemas = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, GreptimeDBTableSchema> schemasByTable = new ConcurrentHashMap<>();
    private final GreptimeDBStorageConfig config;
    private final GreptimeDBIndexPolicy indexPolicy;

    public SchemaRegistry() {
        this(new GreptimeDBStorageConfig());
    }

    public SchemaRegistry(final GreptimeDBStorageConfig config) {
        this.config = config;
        this.indexPolicy = new GreptimeDBIndexPolicy();
    }

    public String getTableName(final Model model) {
        return getWriteSchema(model).getTableName();
    }

    public WriteSchemaInfo getWriteSchema(final Model model) {
        return getWriteSchemas(model).get(0);
    }

    /**
     * The first entry is the main table. Remaining entries are normalized AdditionalEntity tables.
     */
    public List<WriteSchemaInfo> getWriteSchemas(final Model model) {
        return schemas.computeIfAbsent(modelKey(model), ignored -> buildWriteSchemas(model));
    }

    public GreptimeDBTableSchema getTableSchema(final String tableName) {
        final GreptimeDBTableSchema schema = schemasByTable.get(tableName);
        if (schema == null) {
            throw new IllegalStateException("Schema is not registered for table " + tableName);
        }
        return schema;
    }

    private String modelKey(final Model model) {
        return model.getName() + '_' + model.getDownsampling().getValue();
    }

    private List<WriteSchemaInfo> buildWriteSchemas(final Model model) {
        final List<WriteSchemaInfo> result = new ArrayList<>();
        result.add(new WriteSchemaInfo(GreptimeDBTableSchema.forModel(model, config, indexPolicy)));
        model.getSqlDBModelExtension().getAdditionalTables().values().stream()
            .sorted(Comparator.comparing(AdditionalTable::getName))
            .map(table -> GreptimeDBTableSchema.forAdditionalTable(table, config))
            .map(WriteSchemaInfo::new)
            .forEach(result::add);
        for (final WriteSchemaInfo info : result) {
            final GreptimeDBTableSchema previous = schemasByTable.putIfAbsent(
                info.getTableName(), info.getSchema());
            if (previous != null && !previous.fingerprint().equals(info.getFingerprint())) {
                throw new IllegalStateException(
                    "Conflicting schemas for table " + info.getTableName());
            }
        }
        return List.copyOf(result);
    }

    @Getter
    public static class WriteSchemaInfo {
        private final GreptimeDBTableSchema schema;
        private final TableSchema tableSchema;
        private final List<String> columnNames;
        private final List<DataType> dataTypes;
        private final String fingerprint;

        WriteSchemaInfo(final GreptimeDBTableSchema schema) {
            this.schema = schema;
            this.tableSchema = schema.buildWriteSchema();
            this.columnNames = schema.getColumns().stream().map(Column::getName).collect(Collectors.toList());
            this.dataTypes = schema.getColumns().stream().map(Column::getDataType).collect(Collectors.toList());
            this.fingerprint = schema.fingerprint();
        }

        public String getTableName() {
            return schema.getTableName();
        }

        public String getListColumn() {
            return schema.getListColumn();
        }
    }
}
