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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.skywalking.oap.server.core.analysis.DownSampling;
import org.apache.skywalking.oap.server.core.analysis.TimeBucket;
import org.apache.skywalking.oap.server.core.analysis.metrics.DataTable;
import org.apache.skywalking.oap.server.core.query.enumeration.Step;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.core.storage.model.ModelColumn;
import org.apache.skywalking.oap.server.core.storage.type.Convert2Entity;
import org.apache.skywalking.oap.server.core.storage.type.Convert2Storage;
import org.apache.skywalking.oap.server.library.util.CollectionUtils;
import org.apache.skywalking.oap.server.library.util.StringUtil;

/**
 * Converts SkyWalking data model types to GreptimeDB SQL/SDK types and provides
 * Convert2Entity/Convert2Storage implementations for the StorageBuilder pattern.
 */
public final class GreptimeDBConverter {

    private GreptimeDBConverter() {
    }

    /**
     * Quote a column name with backticks for use in SQL (DDL and queries).
     * Required because some column names (e.g. "id") are reserved keywords in GreptimeDB.
     */
    public static String quoteColumn(final String colName) {
        return "`" + colName + "`";
    }

    /**
     * Map a SkyWalking ModelColumn Java type to a GreptimeDB SQL type string (for DDL).
     */
    public static String mapToSqlType(final ModelColumn column) {
        final Class<?> type = column.getType();

        if (String.class.equals(type)) {
            return "STRING";
        } else if (int.class.equals(type) || Integer.class.equals(type)) {
            return "INT";
        } else if (long.class.equals(type) || Long.class.equals(type)) {
            return "BIGINT";
        } else if (double.class.equals(type) || Double.class.equals(type)) {
            return "DOUBLE";
        } else if (float.class.equals(type) || Float.class.equals(type)) {
            return "FLOAT";
        } else if (byte[].class.equals(type)) {
            return "BINARY";
        } else if (DataTable.class.equals(type)) {
            return "STRING";
        } else if (List.class.isAssignableFrom(type)) {
            return "JSON";
        } else if (type.isEnum()) {
            // Enum fields (e.g. Layer, ProfileLanguageType) are stored as int by StorageBuilder
            return "INT";
        } else {
            return "STRING";
        }
    }

    /**
     * Map a SkyWalking ModelColumn Java type to a GreptimeDB SDK DataType (for gRPC writes).
     */
    public static DataType mapDataType(final ModelColumn column) {
        final Class<?> type = column.getType();

        if (String.class.equals(type)) {
            return DataType.String;
        } else if (int.class.equals(type) || Integer.class.equals(type)) {
            return DataType.Int32;
        } else if (long.class.equals(type) || Long.class.equals(type)) {
            return DataType.Int64;
        } else if (double.class.equals(type) || Double.class.equals(type)) {
            return DataType.Float64;
        } else if (float.class.equals(type) || Float.class.equals(type)) {
            return DataType.Float32;
        } else if (byte[].class.equals(type)) {
            return DataType.Binary;
        } else if (DataTable.class.equals(type)) {
            return DataType.String;
        } else if (List.class.isAssignableFrom(type)) {
            return DataType.Json;
        } else if (type.isEnum()) {
            // Enum fields (e.g. Layer, ProfileLanguageType) are stored as int by StorageBuilder
            return DataType.Int32;
        } else {
            return DataType.String;
        }
    }

    /**
     * Resolve the GreptimeDB table name for a SkyWalking Model.
     * Metrics get a downsampling suffix (_minute, _hour, _day).
     */
    public static String resolveTableName(final Model model) {
        if (model.isTimeSeries() && model.isMetric()) {
            final DownSampling ds = model.getDownsampling();
            if (ds != DownSampling.None) {
                return model.getName() + "_" + ds.getName();
            }
        }
        return model.getName();
    }

    /**
     * Convert a SkyWalking time_bucket to Unix epoch milliseconds.
     */
    public static long timeBucketToTimestamp(final long timeBucket, final DownSampling downsampling) {
        return TimeBucket.getTimestamp(timeBucket, downsampling);
    }

    /**
     * Convert a SkyWalking time_bucket to Unix epoch milliseconds (auto-detect downsampling).
     */
    public static long timeBucketToTimestamp(final long timeBucket) {
        return TimeBucket.getTimestamp(timeBucket);
    }

    /**
     * Resolve the GreptimeDB table name for a metrics query. Appends the downsampling suffix
     * based on the Duration step.
     */
    public static String resolveMetricsTableName(final String metricName, final Step step) {
        return metricName + "_" + step.name().toLowerCase();
    }

    /**
     * Resolve the GreptimeDB table name for non-downsampled Metrics (traffic/metadata).
     * MetricsStreamProcessor always registers them with DownSampling.Minute,
     * so the actual GreptimeDB table has a "_minute" suffix.
     */
    public static String resolveTrafficTableName(final String indexName) {
        return indexName + "_minute";
    }

    /**
     * Build a Map from a JDBC ResultSet row using ResultSetMetaData (no Model needed).
     * Suitable for metadata queries where the Model object is not available.
     */
    public static Map<String, Object> resultSetToGenericMap(final ResultSet rs) throws SQLException {
        final Map<String, Object> map = new HashMap<>();
        final ResultSetMetaData meta = rs.getMetaData();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            final int sqlType = meta.getColumnType(i);
            if (sqlType == java.sql.Types.BINARY
                || sqlType == java.sql.Types.VARBINARY
                || sqlType == java.sql.Types.LONGVARBINARY) {
                map.put(meta.getColumnName(i), rs.getBytes(i));
            } else {
                map.put(meta.getColumnName(i), rs.getObject(i));
            }
        }
        return map;
    }

    /**
     * Select PRIMARY KEY columns for a model. Shared by TableInstaller and SchemaRegistry.
     */
    public static List<String> selectPrimaryKeyColumns(final Model model) {
        final List<String> pkColumns = new ArrayList<>();

        if (!model.isTimeSeries()) {
            // id is synthetic (StorageData.id().build()), always use it as PK for non-timeSeries
            pkColumns.add("id");
            return pkColumns;
        }

        if (model.isMetric()) {
            for (final ModelColumn col : model.getColumns()) {
                final String name = col.getColumnName().getStorageName();
                if (name.equals("service_id") || name.equals("entity_id")) {
                    pkColumns.add(name);
                }
                if (pkColumns.size() >= 2) {
                    break;
                }
            }
            if (pkColumns.isEmpty()) {
                for (final ModelColumn col : model.getColumns()) {
                    if (String.class.equals(col.getType()) && !col.isStorageOnly()
                        && !isHighCardinalityColumn(col.getColumnName().getStorageName())) {
                        pkColumns.add(col.getColumnName().getStorageName());
                        break;
                    }
                }
            }
        } else if (model.isRecord()) {
            for (final ModelColumn col : model.getColumns()) {
                if ("service_id".equals(col.getColumnName().getStorageName())) {
                    pkColumns.add("service_id");
                    break;
                }
            }
        }

        return pkColumns;
    }

    /**
     * Check if a column is high-cardinality (should not be a PRIMARY KEY).
     */
    public static boolean isHighCardinalityColumn(final String colName) {
        return colName.equals("trace_id")
            || colName.equals("segment_id")
            || colName.equals("unique_id");
    }

    /**
     * Convert a List of "key=value" tag strings to a JSON object string.
     * Example: ["http.method=GET", "http.status_code=200"] -> {"http.method":"GET","http.status_code":"200"}
     */
    public static String tagsToJson(final List<String> tags) {
        if (CollectionUtils.isEmpty(tags)) {
            return null;
        }
        final StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (final String tag : tags) {
            final int idx = tag.indexOf('=');
            if (idx == 0) {
                // Skip malformed "=value" entries (empty key)
                continue;
            }
            if (!first) {
                sb.append(',');
            }
            first = false;
            if (idx < 0) {
                // No '=' found: store entire string as key with empty value (e.g. Zipkin annotations)
                sb.append('"').append(escapeJson(tag)).append("\":\"\"");
            } else {
                sb.append('"').append(escapeJson(tag.substring(0, idx)))
                  .append("\":\"").append(escapeJson(tag.substring(idx + 1)))
                  .append('"');
            }
        }
        sb.append('}');
        return sb.toString();
    }

    private static String escapeJson(final String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    /**
     * Build an ordered column map (name -> value) from a storage map using the Model's column order.
     * Adds the greptime_ts timestamp column at the end.
     */
    public static LinkedHashMap<String, Object> buildOrderedRow(
            final Map<String, Object> storageMap,
            final Model model,
            final long greptimeTs) {
        final LinkedHashMap<String, Object> row = new LinkedHashMap<>();
        // Synthetic id column comes first (populated by caller)
        if (storageMap.containsKey("id")) {
            row.put("id", storageMap.get("id"));
        }
        for (final ModelColumn col : model.getColumns()) {
            final String colName = col.getColumnName().getStorageName();
            row.put(colName, storageMap.get(colName));
        }
        row.put("greptime_ts", greptimeTs);
        return row;
    }

    /**
     * HashMap-based Convert2Entity, reads values from a Map populated by JDBC ResultSet.
     */
    @RequiredArgsConstructor
    public static class ToEntity implements Convert2Entity {
        private final Map<String, Object> source;

        @Override
        public Object get(final String fieldName) {
            return source.get(fieldName);
        }

        @Override
        public byte[] getBytes(final String fieldName) {
            final Object value = source.get(fieldName);
            if (value == null) {
                return new byte[0];
            }
            if (value instanceof byte[]) {
                return (byte[]) value;
            }
            // GreptimeDB stores BINARY columns directly (not Base64-encoded).
            // When the JDBC driver returns a String, convert it back to bytes using UTF-8.
            final String str = value.toString();
            if (StringUtil.isEmpty(str)) {
                return new byte[0];
            }
            return str.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }
    }

    /**
     * HashMap-based Convert2Storage, collects field values into a Map.
     * {@code List<String>} fields (tags) are automatically converted to JSON.
     */
    public static class ToStorage implements Convert2Storage<Map<String, Object>> {
        private final Map<String, Object> source = new HashMap<>();

        @Override
        public void accept(final String fieldName, final Object fieldValue) {
            source.put(fieldName, fieldValue);
        }

        @Override
        public void accept(final String fieldName, final byte[] fieldValue) {
            source.put(fieldName, fieldValue);
        }

        @Override
        public void accept(final String fieldName, final List<String> fieldValue) {
            // Convert tag list to JSON string for GreptimeDB JSON column
            source.put(fieldName, tagsToJson(fieldValue));
        }

        @Override
        public Object get(final String fieldName) {
            return source.get(fieldName);
        }

        @Override
        public Map<String, Object> obtain() {
            return source;
        }
    }

    /**
     * Build a Map from a JDBC ResultSet row using the Model's column definitions.
     */
    public static Map<String, Object> resultSetToMap(final ResultSet rs, final Model model) throws SQLException {
        final Map<String, Object> map = new HashMap<>();
        for (final ModelColumn col : model.getColumns()) {
            final String colName = col.getColumnName().getStorageName();
            final Class<?> type = col.getType();
            if (byte[].class.equals(type)) {
                map.put(colName, rs.getBytes(colName));
            } else if (int.class.equals(type) || Integer.class.equals(type)) {
                map.put(colName, rs.getInt(colName));
            } else if (long.class.equals(type) || Long.class.equals(type)) {
                map.put(colName, rs.getLong(colName));
            } else if (double.class.equals(type) || Double.class.equals(type)) {
                map.put(colName, rs.getDouble(colName));
            } else if (float.class.equals(type) || Float.class.equals(type)) {
                map.put(colName, rs.getFloat(colName));
            } else if (type.isEnum()) {
                // Enum fields are stored as int
                map.put(colName, rs.getInt(colName));
            } else {
                map.put(colName, rs.getString(colName));
            }
        }
        return map;
    }
}
