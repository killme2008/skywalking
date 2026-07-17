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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.skywalking.oap.server.core.analysis.DownSampling;
import org.apache.skywalking.oap.server.core.analysis.Layer;
import org.apache.skywalking.oap.server.core.analysis.metrics.DataTable;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.core.storage.model.ModelColumn;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GreptimeDBConverterTest {

    // ---- quoteColumn ----

    @Test
    void quoteColumnShouldWrapWithBackticks() {
        assertEquals("`id`", GreptimeDBConverter.quoteColumn("id"));
        assertEquals("`service_id`", GreptimeDBConverter.quoteColumn("service_id"));
        assertEquals("`greptime_ts`", GreptimeDBConverter.quoteColumn("greptime_ts"));
    }

    // ---- mapToSqlType ----

    @Test
    void mapToSqlTypeShouldReturnStringForStringColumn() {
        assertEquals("STRING", GreptimeDBConverter.mapToSqlType(TestModels.col("name", String.class)));
    }

    @Test
    void mapToSqlTypeShouldReturnIntForIntColumn() {
        assertEquals("INT", GreptimeDBConverter.mapToSqlType(TestModels.col("count", int.class)));
        assertEquals("INT", GreptimeDBConverter.mapToSqlType(TestModels.col("count", Integer.class)));
    }

    @Test
    void mapToSqlTypeShouldReturnBigintForLongColumn() {
        assertEquals("BIGINT", GreptimeDBConverter.mapToSqlType(TestModels.col("time_bucket", long.class)));
        assertEquals("BIGINT", GreptimeDBConverter.mapToSqlType(TestModels.col("time_bucket", Long.class)));
    }

    @Test
    void mapToSqlTypeShouldReturnDoubleForDoubleColumn() {
        assertEquals("DOUBLE", GreptimeDBConverter.mapToSqlType(TestModels.col("value", double.class)));
        assertEquals("DOUBLE", GreptimeDBConverter.mapToSqlType(TestModels.col("value", Double.class)));
    }

    @Test
    void mapToSqlTypeShouldReturnFloatForFloatColumn() {
        assertEquals("FLOAT", GreptimeDBConverter.mapToSqlType(TestModels.col("ratio", float.class)));
        assertEquals("FLOAT", GreptimeDBConverter.mapToSqlType(TestModels.col("ratio", Float.class)));
    }

    @Test
    void mapToSqlTypeShouldReturnBinaryForByteArray() {
        assertEquals("BINARY", GreptimeDBConverter.mapToSqlType(TestModels.col("data_binary", byte[].class)));
    }

    @Test
    void mapToSqlTypeShouldReturnStringForDataTable() {
        assertEquals("STRING", GreptimeDBConverter.mapToSqlType(TestModels.col("percentile", DataTable.class)));
    }

    @Test
    void mapToSqlTypeShouldRejectListColumns() {
        assertThrows(
            IllegalArgumentException.class,
            () -> GreptimeDBConverter.mapToSqlType(TestModels.col("tags", List.class))
        );
    }

    // ---- mapDataType ----

    @Test
    void mapDataTypeShouldReturnCorrectGrpcTypes() {
        assertEquals(DataType.String, GreptimeDBConverter.mapDataType(TestModels.col("name", String.class)));
        assertEquals(DataType.Int32, GreptimeDBConverter.mapDataType(TestModels.col("count", int.class)));
        assertEquals(DataType.Int32, GreptimeDBConverter.mapDataType(TestModels.col("count", Integer.class)));
        assertEquals(DataType.Int64, GreptimeDBConverter.mapDataType(TestModels.col("time_bucket", long.class)));
        assertEquals(DataType.Int64, GreptimeDBConverter.mapDataType(TestModels.col("time_bucket", Long.class)));
        assertEquals(DataType.Float64, GreptimeDBConverter.mapDataType(TestModels.col("value", double.class)));
        assertEquals(DataType.Float32, GreptimeDBConverter.mapDataType(TestModels.col("ratio", float.class)));
        assertEquals(DataType.Binary, GreptimeDBConverter.mapDataType(TestModels.col("data", byte[].class)));
        assertEquals(DataType.String, GreptimeDBConverter.mapDataType(TestModels.col("dt", DataTable.class)));
        assertThrows(
            IllegalArgumentException.class,
            () -> GreptimeDBConverter.mapDataType(TestModels.col("tags", List.class))
        );
    }

    // ---- resolveTableName ----

    @Test
    void resolveTableNameShouldAppendDownsamplingSuffixForMetrics() {
        final Model model = TestModels.sampleMetricsModel();
        assertEquals("service_resp_time_minute", GreptimeDBConverter.resolveTableName(model));
    }

    @Test
    void resolveTableNameShouldReturnPlainNameForRecords() {
        final Model model = TestModels.sampleRecordModel();
        assertEquals("segment", GreptimeDBConverter.resolveTableName(model));
    }

    @Test
    void resolveTableNameShouldReturnPlainNameForManagement() {
        final Model model = TestModels.sampleManagementModel();
        assertEquals("ui_template", GreptimeDBConverter.resolveTableName(model));
    }

    @Test
    void resolveTableNameShouldReturnPlainNameForMetricsWithNoneDownsampling() {
        final List<ModelColumn> columns = new ArrayList<>();
        columns.add(TestModels.col("service_id", String.class));
        final Model model = TestModels.metricsModel("some_metric", DownSampling.None, columns);
        assertEquals("some_metric", GreptimeDBConverter.resolveTableName(model));
    }

    @Test
    void resolveTableNameShouldAppendHourForHourDownsampling() {
        final List<ModelColumn> columns = new ArrayList<>();
        columns.add(TestModels.col("service_id", String.class));
        final Model model = TestModels.metricsModel("service_resp_time", DownSampling.Hour, columns);
        assertEquals("service_resp_time_hour", GreptimeDBConverter.resolveTableName(model));
    }

    // ---- selectPrimaryKeyColumns ----

    @Test
    void selectPrimaryKeyColumnsShouldReturnIdForManagement() {
        final Model model = TestModels.sampleManagementModel();
        final List<String> pk = GreptimeDBConverter.selectPrimaryKeyColumns(model);
        assertEquals(Collections.singletonList("id"), pk);
    }

    @Test
    void selectPrimaryKeyColumnsShouldReturnServiceIdForNoneStream() {
        final List<ModelColumn> columns = new ArrayList<>();
        columns.add(TestModels.col("task_id", String.class));
        columns.add(TestModels.col("service_id", String.class));
        final Model model = TestModels.noneStreamModel("profile_task", columns);
        final List<String> pk = GreptimeDBConverter.selectPrimaryKeyColumns(model);
        assertEquals(Collections.singletonList("service_id"), pk);
    }

    @Test
    void selectPrimaryKeyColumnsShouldReturnEntityIdForMetrics() {
        final Model model = TestModels.sampleMetricsModel();
        final List<String> pk = GreptimeDBConverter.selectPrimaryKeyColumns(model);
        assertEquals(Collections.singletonList("entity_id"), pk);
    }

    @Test
    void selectPrimaryKeyColumnsShouldReturnServiceIdForRecords() {
        final Model model = TestModels.sampleRecordModel();
        final List<String> pk = GreptimeDBConverter.selectPrimaryKeyColumns(model);
        assertEquals(Collections.singletonList("service_id"), pk);
    }

    @Test
    void selectPrimaryKeyColumnsShouldUseIdForMetricsWithoutEntityId() {
        final List<ModelColumn> columns = new ArrayList<>();
        columns.add(TestModels.col("custom_key", String.class));
        columns.add(TestModels.col("value", long.class, true, 0));
        final Model model = TestModels.metricsModel("custom_metric", DownSampling.Minute, columns);
        final List<String> pk = GreptimeDBConverter.selectPrimaryKeyColumns(model);
        assertEquals(Collections.singletonList("id"), pk);
    }

    @Test
    void selectPrimaryKeyColumnsShouldUseIdEvenWithHighCardinalityColumns() {
        final List<ModelColumn> columns = new ArrayList<>();
        columns.add(TestModels.col("trace_id", String.class));
        columns.add(TestModels.col("real_key", String.class));
        final Model model = TestModels.metricsModel("odd_metric", DownSampling.Minute, columns);
        final List<String> pk = GreptimeDBConverter.selectPrimaryKeyColumns(model);
        assertEquals(Collections.singletonList("id"), pk);
    }

    // ---- resolveMetricsTableName ----

    @Test
    void resolveMetricsTableNameShouldAppendStepSuffix() {
        assertEquals("service_resp_time_minute",
            GreptimeDBConverter.resolveMetricsTableName("service_resp_time",
                org.apache.skywalking.oap.server.core.query.enumeration.Step.MINUTE));
        assertEquals("service_resp_time_hour",
            GreptimeDBConverter.resolveMetricsTableName("service_resp_time",
                org.apache.skywalking.oap.server.core.query.enumeration.Step.HOUR));
        assertEquals("service_resp_time_day",
            GreptimeDBConverter.resolveMetricsTableName("service_resp_time",
                org.apache.skywalking.oap.server.core.query.enumeration.Step.DAY));
    }

    // ---- buildOrderedRow ----

    @Test
    void buildOrderedRowShouldIncludeIdAndGreptimeTs() {
        final Model model = TestModels.sampleMetricsModel();
        final Map<String, Object> storageMap = new HashMap<>();
        storageMap.put("id", "test-id-123");
        storageMap.put("service_id", "svc1");
        storageMap.put("entity_id", "ent1");
        storageMap.put("time_bucket", 202401011200L);
        storageMap.put("summation", 100L);
        storageMap.put("count", 10L);
        storageMap.put("value", 10L);

        final long ts = 1704067200000L;
        final LinkedHashMap<String, Object> row = GreptimeDBConverter.buildOrderedRow(storageMap, model, ts);

        // id should be first
        final List<String> keys = new ArrayList<>(row.keySet());
        assertEquals("id", keys.get(0));
        assertEquals("test-id-123", row.get("id"));

        // greptime_ts should be last
        assertEquals("greptime_ts", keys.get(keys.size() - 1));
        assertEquals(ts, row.get("greptime_ts"));

        // Model columns should be present
        assertEquals("svc1", row.get("service_id"));
        assertEquals("ent1", row.get("entity_id"));
        assertEquals(100L, row.get("summation"));
    }

    @Test
    void buildOrderedRowShouldOmitIdWhenNotInStorageMap() {
        final Model model = TestModels.sampleMetricsModel();
        final Map<String, Object> storageMap = new HashMap<>();
        storageMap.put("service_id", "svc1");
        storageMap.put("entity_id", "ent1");
        storageMap.put("time_bucket", 202401011200L);
        storageMap.put("summation", 100L);
        storageMap.put("count", 10L);
        storageMap.put("value", 10L);

        final LinkedHashMap<String, Object> row = GreptimeDBConverter.buildOrderedRow(storageMap, model, 0L);
        assertFalse(row.containsKey("id"));
    }

    // ---- ToStorage ----

    @Test
    void toStorageShouldPreserveTagListForAdditionalTableRows() {
        final GreptimeDBConverter.ToStorage storage = new GreptimeDBConverter.ToStorage();
        final List<String> tags = Arrays.asList("k1=v1", "k2=v2");
        storage.accept("tags", tags);
        assertEquals(tags, storage.get("tags"));
    }

    @Test
    void toStorageShouldPreserveEmptyTagList() {
        final GreptimeDBConverter.ToStorage storage = new GreptimeDBConverter.ToStorage();
        storage.accept("tags", Collections.emptyList());
        assertEquals(Collections.emptyList(), storage.get("tags"));
    }

    // ---- ToEntity ----

    @Test
    void toEntityShouldReturnStoredValues() {
        final Map<String, Object> map = new HashMap<>();
        map.put("service_id", "svc1");
        map.put("latency", 42);
        final GreptimeDBConverter.ToEntity entity = new GreptimeDBConverter.ToEntity(map);
        assertEquals("svc1", entity.get("service_id"));
        assertEquals(42, entity.get("latency"));
    }

    @Test
    void toEntityGetBytesShouldReturnByteArray() {
        final byte[] data = new byte[]{1, 2, 3};
        final Map<String, Object> map = new HashMap<>();
        map.put("data_binary", data);
        final GreptimeDBConverter.ToEntity entity = new GreptimeDBConverter.ToEntity(map);
        assertEquals(data, entity.getBytes("data_binary"));
    }

    @Test
    void toEntityGetBytesShouldReturnEmptyArrayForNull() {
        final Map<String, Object> map = new HashMap<>();
        map.put("data_binary", null);
        final GreptimeDBConverter.ToEntity entity = new GreptimeDBConverter.ToEntity(map);
        assertEquals(0, entity.getBytes("data_binary").length);
    }

    @Test
    void toEntityGetBytesShouldReturnUtf8BytesForString() {
        final Map<String, Object> map = new HashMap<>();
        // GreptimeDB stores BINARY columns directly (not Base64-encoded),
        // so getBytes() returns raw UTF-8 bytes of the string.
        map.put("data_binary", "abc");
        final GreptimeDBConverter.ToEntity entity = new GreptimeDBConverter.ToEntity(map);
        final byte[] bytes = entity.getBytes("data_binary");
        assertEquals(3, bytes.length);
        assertEquals('a', bytes[0]);
        assertEquals('b', bytes[1]);
        assertEquals('c', bytes[2]);
    }

    // ---- mapToSqlType / mapDataType for enum types ----

    @Test
    void mapToSqlTypeShouldReturnIntForEnumColumn() {
        assertEquals("INT", GreptimeDBConverter.mapToSqlType(TestModels.col("layer", Layer.class)));
    }

    @Test
    void mapDataTypeShouldReturnInt32ForEnumColumn() {
        assertEquals(DataType.Int32, GreptimeDBConverter.mapDataType(TestModels.col("layer", Layer.class)));
    }

    // ---- resolveTrafficTableName ----

    @Test
    void resolveTrafficTableNameShouldAppendMinuteSuffix() {
        assertEquals("network_address_alias_minute",
            GreptimeDBConverter.resolveTrafficTableName("network_address_alias"));
        assertEquals("service_traffic_minute",
            GreptimeDBConverter.resolveTrafficTableName("service_traffic"));
    }

    @Test
    void storageTimestampShouldCollapseIndexModeMetricsToOneRowPerHour() {
        final Model model = TestModels.indexModeMetricsModel(
            "ebpf_profiling_schedule", DownSampling.Minute,
            Collections.singletonList(TestModels.col("task_id", String.class)));

        final long firstMinute = GreptimeDBConverter.storageTimestamp(model, 202401011001L);
        final long lastMinute = GreptimeDBConverter.storageTimestamp(model, 202401011059L);
        final long nextHour = GreptimeDBConverter.storageTimestamp(model, 202401011100L);

        assertEquals(firstMinute, lastMinute);
        assertFalse(firstMinute == nextHour);
        assertEquals(
            org.apache.skywalking.oap.server.core.analysis.TimeBucket.getTimestamp(
                2024010110L, DownSampling.Hour),
            firstMinute);
    }

    @Test
    void storageTimestampShouldUseIndexModeInsteadOfModelName() {
        final Model model = TestModels.metricsModel(
            "service_traffic", DownSampling.Minute,
            Collections.singletonList(TestModels.col("service_id", String.class)));

        assertEquals(
            GreptimeDBConverter.timeBucketToTimestamp(202401011001L, DownSampling.Minute),
            GreptimeDBConverter.storageTimestamp(model, 202401011001L));
    }

    @Test
    void storageTimestampShouldPreserveNormalMetricResolution() {
        final Model model = TestModels.sampleMetricsModel();
        assertEquals(
            GreptimeDBConverter.timeBucketToTimestamp(202401011234L, DownSampling.Minute),
            GreptimeDBConverter.storageTimestamp(model, 202401011234L));
    }

    @Test
    void metricPrimaryKeyShouldContainAllSeriesIdColumnsInDeclaredOrder() {
        final Model model = TestModels.metricsModel(
            "relation_metric",
            DownSampling.Minute,
            Arrays.asList(
                TestModels.seriesIdCol("component_id", int.class, 1),
                TestModels.seriesIdCol("entity_id", String.class, 0),
                TestModels.col("value", long.class)
            )
        );

        assertEquals(
            Arrays.asList("entity_id", "component_id"),
            GreptimeDBConverter.selectPrimaryKeyColumns(model)
        );
    }

}
