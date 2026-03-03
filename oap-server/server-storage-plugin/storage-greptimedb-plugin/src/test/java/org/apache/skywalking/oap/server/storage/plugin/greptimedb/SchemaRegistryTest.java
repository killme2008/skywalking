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
import java.util.List;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaRegistryTest {

    private SchemaRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new SchemaRegistry();
    }

    // ---- getTableName ----

    @Test
    void getTableNameShouldResolveMetricsWithSuffix() {
        final Model model = TestModels.sampleMetricsModel();
        assertEquals("service_resp_time_minute", registry.getTableName(model));
    }

    @Test
    void getTableNameShouldResolveRecordWithoutSuffix() {
        final Model model = TestModels.sampleRecordModel();
        assertEquals("segment", registry.getTableName(model));
    }

    @Test
    void getTableNameShouldCacheResult() {
        final Model model = TestModels.sampleMetricsModel();
        final String first = registry.getTableName(model);
        final String second = registry.getTableName(model);
        assertSame(first, second, "Cached table name should return same instance");
    }

    // ---- getWriteSchema ----

    @Test
    void getWriteSchemaShouldBuildForMetricsModel() {
        final Model model = TestModels.sampleMetricsModel();
        final SchemaRegistry.WriteSchemaInfo info = registry.getWriteSchema(model);

        assertNotNull(info);
        assertNotNull(info.getTableSchema());

        final List<String> colNames = info.getColumnNames();
        // First column should be id
        assertEquals("id", colNames.get(0), "First column should be synthetic id");
        // Last column should be greptime_ts
        assertEquals("greptime_ts", colNames.get(colNames.size() - 1),
            "Last column should be greptime_ts");
        // Should contain all model columns
        assertTrue(colNames.contains("service_id"));
        assertTrue(colNames.contains("entity_id"));
        assertTrue(colNames.contains("time_bucket"));
        assertTrue(colNames.contains("summation"));
        assertTrue(colNames.contains("count"));
        assertTrue(colNames.contains("value"));
    }

    @Test
    void getWriteSchemaShouldBuildForManagementModel() {
        final Model model = TestModels.sampleManagementModel();
        final SchemaRegistry.WriteSchemaInfo info = registry.getWriteSchema(model);

        assertNotNull(info);
        final List<String> colNames = info.getColumnNames();

        assertEquals("id", colNames.get(0));
        assertEquals("greptime_ts", colNames.get(colNames.size() - 1));
        assertTrue(colNames.contains("name"));
        assertTrue(colNames.contains("type"));
        assertTrue(colNames.contains("configuration"));
    }

    @Test
    void getWriteSchemaShouldBuildForRecordModel() {
        final Model model = TestModels.sampleRecordModel();
        final SchemaRegistry.WriteSchemaInfo info = registry.getWriteSchema(model);

        assertNotNull(info);
        final List<String> colNames = info.getColumnNames();

        assertEquals("id", colNames.get(0));
        assertEquals("greptime_ts", colNames.get(colNames.size() - 1));
        assertTrue(colNames.contains("segment_id"));
        assertTrue(colNames.contains("trace_id"));
        assertTrue(colNames.contains("service_id"));
    }

    @Test
    void getWriteSchemaShouldCacheResult() {
        final Model model = TestModels.sampleMetricsModel();
        final SchemaRegistry.WriteSchemaInfo first = registry.getWriteSchema(model);
        final SchemaRegistry.WriteSchemaInfo second = registry.getWriteSchema(model);
        assertSame(first, second, "Cached write schema should return same instance");
    }

    @Test
    void getWriteSchemaColumnCountShouldMatchModelPlusIdAndTimestamp() {
        final Model model = TestModels.sampleMetricsModel();
        final SchemaRegistry.WriteSchemaInfo info = registry.getWriteSchema(model);
        // model columns + id + greptime_ts
        final int expected = model.getColumns().size() + 2;
        assertEquals(expected, info.getColumnNames().size());
    }

    // ---- dataTypes list ----

    @Test
    void getWriteSchemaDataTypesShouldMatchColumnNames() {
        final Model model = TestModels.sampleMetricsModel();
        final SchemaRegistry.WriteSchemaInfo info = registry.getWriteSchema(model);

        final List<String> colNames = info.getColumnNames();
        final List<DataType> dataTypes = info.getDataTypes();

        assertEquals(colNames.size(), dataTypes.size(),
            "dataTypes list size must match columnNames list size");

        // Verify specific types: id -> String, service_id -> String,
        // time_bucket -> Int64, summation -> Int64, greptime_ts -> TimestampMillisecond
        assertEquals(DataType.String, dataTypes.get(colNames.indexOf("id")));
        assertEquals(DataType.String, dataTypes.get(colNames.indexOf("service_id")));
        assertEquals(DataType.Int64, dataTypes.get(colNames.indexOf("time_bucket")));
        assertEquals(DataType.Int64, dataTypes.get(colNames.indexOf("summation")));
        assertEquals(DataType.TimestampMillisecond, dataTypes.get(colNames.indexOf("greptime_ts")));
    }
}
