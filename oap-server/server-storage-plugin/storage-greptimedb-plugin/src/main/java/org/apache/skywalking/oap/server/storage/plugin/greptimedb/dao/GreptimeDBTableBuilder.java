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
        // Collect all field values from entity via StorageBuilder
        final GreptimeDBConverter.ToStorage converter = new GreptimeDBConverter.ToStorage();
        storageBuilder.entity2Storage(entity, converter);
        final Map<String, Object> storageMap = converter.obtain();

        // Build row values in schema column order, coercing types to match SDK expectations
        final List<String> columnNames = schemaInfo.getColumnNames();
        final List<DataType> dataTypes = schemaInfo.getDataTypes();
        final Object[] row = new Object[columnNames.size()];
        for (int i = 0; i < columnNames.size(); i++) {
            final String colName = columnNames.get(i);
            final DataType expectedType = dataTypes.get(i);
            if ("greptime_ts".equals(colName)) {
                row[i] = greptimeTs;
            } else if ("id".equals(colName)) {
                // Synthetic id: computed by StorageData, not part of StorageBuilder output
                row[i] = entity.id().build();
            } else {
                row[i] = coerceValue(storageMap.get(colName), expectedType);
            }
        }

        final Table table = Table.from(schemaInfo.getTableSchema());
        table.addRow(row);
        return table;
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
