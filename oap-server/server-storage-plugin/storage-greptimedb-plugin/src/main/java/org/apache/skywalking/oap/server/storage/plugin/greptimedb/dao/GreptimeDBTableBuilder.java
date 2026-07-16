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

package org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao;

import io.greptime.models.DataType;
import io.greptime.models.Table;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.skywalking.oap.server.core.storage.StorageData;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.core.storage.type.StorageBuilder;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBConverter;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.SchemaRegistry;

/**
 * Utility for building a GreptimeDB gRPC Table from a SkyWalking entity using the StorageBuilder pattern.
 */
public final class GreptimeDBTableBuilder {

    private GreptimeDBTableBuilder() {
    }

    /**
     * Convert a SkyWalking entity to a single-row gRPC Table.
     *
     * @param entity        the SkyWalking storage data entity
     * @param storageBuilder the StorageBuilder for this entity type
     * @param model         the Model definition
     * @param schemaInfo    the cached write schema
     * @param greptimeTs    the timestamp value for greptime_ts column (epoch millis)
     * @return a Table with one row, ready to be written via gRPC
     */
    @SuppressWarnings("unchecked")
    public static <T extends StorageData> Table buildTable(
            final T entity,
            final StorageBuilder<T> storageBuilder,
            final Model model,
            final SchemaRegistry.WriteSchemaInfo schemaInfo,
            final long greptimeTs) {
        final List<GreptimeDBPreparedRow> rows = buildRows(
            entity, storageBuilder, model, java.util.Collections.singletonList(schemaInfo), greptimeTs);
        final Table table = Table.from(schemaInfo.getTableSchema());
        table.addRow(rows.get(0).getValues());
        return table;
    }

    /**
     * Convert one entity into a main-table row and zero or more normalized AdditionalEntity rows.
     */
    @SuppressWarnings("unchecked")
    public static <T extends StorageData> List<GreptimeDBPreparedRow> buildRows(
            final T entity,
            final StorageBuilder<T> storageBuilder,
            final Model model,
            final List<SchemaRegistry.WriteSchemaInfo> schemas,
            final long greptimeTs) {
        final GreptimeDBConverter.ToStorage converter = new GreptimeDBConverter.ToStorage();
        storageBuilder.entity2Storage(entity, converter);
        final Map<String, Object> storageMap = converter.obtain();
        final List<GreptimeDBPreparedRow> rows = new ArrayList<>();

        rows.add(new GreptimeDBPreparedRow(
            schemas.get(0), buildRow(entity, storageMap, schemas.get(0), greptimeTs, null)));
        for (int i = 1; i < schemas.size(); i++) {
            final SchemaRegistry.WriteSchemaInfo schema = schemas.get(i);
            final Object value = storageMap.get(schema.getListColumn());
            if (!(value instanceof List)) {
                continue;
            }
            for (final Object item : (List<?>) value) {
                if (item == null) {
                    continue;
                }
                final String listItem = item.toString();
                rows.add(new GreptimeDBPreparedRow(
                    schema, buildRow(entity, storageMap, schema, greptimeTs, listItem)));
            }
        }
        return rows;
    }

    private static <T extends StorageData> Object[] buildRow(
            final T entity,
            final Map<String, Object> storageMap,
            final SchemaRegistry.WriteSchemaInfo schemaInfo,
            final long greptimeTs,
            final String listItem) {
        final List<String> columnNames = schemaInfo.getColumnNames();
        final List<DataType> dataTypes = schemaInfo.getDataTypes();
        final Object[] row = new Object[columnNames.size()];
        for (int i = 0; i < columnNames.size(); i++) {
            final String colName = columnNames.get(i);
            final DataType expectedType = dataTypes.get(i);
            if ("greptime_ts".equals(colName)) {
                row[i] = greptimeTs;
            } else if ("id".equals(colName)) {
                row[i] = entity.id().build();
            } else if (colName.equals(schemaInfo.getListColumn())) {
                row[i] = listItem;
            } else {
                row[i] = coerceValue(storageMap.get(colName), expectedType);
            }
        }
        return row;
    }

    /**
     * Coerce a value to match the GreptimeDB SDK's expected Java type for the given DataType.
     * The SDK's RowHelper.addValue() performs strict casts (e.g. (String) for String,
     * (int) for Int32), so we must ensure the value is the exact expected type.
     */
    static Object coerceValue(final Object value, final DataType expectedType) {
        if (value == null) {
            return null;
        }
        if (value instanceof org.apache.skywalking.oap.server.core.storage.type.StorageDataComplexObject) {
            return ((org.apache.skywalking.oap.server.core.storage.type.StorageDataComplexObject<?>) value)
                .toStorageData();
        }
        switch (expectedType) {
            case String:
                return value instanceof String ? value : value.toString();
            case Int32:
                if (value instanceof Integer) {
                    return value;
                }
                if (value instanceof Number) {
                    return ((Number) value).intValue();
                }
                return value;
            case Int64:
                // SDK uses ValueUtil.getLongValue() which handles Integer/Long/Number
                return value;
            case Float64:
                if (value instanceof Double) {
                    return value;
                }
                if (value instanceof Number) {
                    return ((Number) value).doubleValue();
                }
                return value;
            case Float32:
                if (value instanceof Float) {
                    return value;
                }
                if (value instanceof Number) {
                    return ((Number) value).floatValue();
                }
                return value;
            default:
                return value;
        }
    }
}
