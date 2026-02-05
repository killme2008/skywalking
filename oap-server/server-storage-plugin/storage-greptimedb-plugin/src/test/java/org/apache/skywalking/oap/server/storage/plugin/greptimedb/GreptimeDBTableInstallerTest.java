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

import java.util.ArrayList;
import java.util.List;
import org.apache.skywalking.oap.server.core.analysis.DownSampling;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.core.storage.model.ModelColumn;
import org.apache.skywalking.oap.server.library.module.ModuleManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
class GreptimeDBTableInstallerTest {

    @Mock
    private GreptimeDBStorageClient client;
    @Mock
    private ModuleManager moduleManager;

    private GreptimeDBStorageConfig config;
    private GreptimeDBTableInstaller installer;

    @BeforeEach
    void setUp() {
        config = new GreptimeDBStorageConfig();
        config.setMetricsTTL("7d");
        config.setRecordsTTL("3d");
        config.setManagementTTL("0");
        installer = new GreptimeDBTableInstaller(client, moduleManager, config);
    }

    // ---- DDL: Metrics model ----

    @Test
    void buildDDLForMetricsShouldContainMergeModeAndTTL() {
        final Model model = TestModels.sampleMetricsModel();
        final String ddl = installer.buildCreateTableDDL(model);

        assertTrue(ddl.contains("CREATE TABLE IF NOT EXISTS service_resp_time_minute"),
            "Should use table name with downsampling suffix");
        assertTrue(ddl.contains("`id` STRING"), "Should include synthetic id column");
        assertTrue(ddl.contains("`service_id` STRING"), "Should include service_id");
        assertTrue(ddl.contains("`entity_id` STRING"), "Should include entity_id");
        assertTrue(ddl.contains("`time_bucket` BIGINT"), "Should include time_bucket");
        assertTrue(ddl.contains("`summation` BIGINT"), "Should include summation");
        assertTrue(ddl.contains("`greptime_ts` TIMESTAMP TIME INDEX"), "Should include TIME INDEX");
        assertTrue(ddl.contains("PRIMARY KEY (`service_id`, `entity_id`)"),
            "Should have service_id and entity_id as PRIMARY KEY");
        assertTrue(ddl.contains("'merge_mode' = 'last_row'"), "Metrics should use merge_mode");
        assertTrue(ddl.contains("'ttl' = '7d'"), "Should have metrics TTL");
        assertFalse(ddl.contains("append_mode"), "Metrics should not use append_mode");
    }

    @Test
    void buildDDLForMetricsShouldHaveTimestampWithoutDefault() {
        final Model model = TestModels.sampleMetricsModel();
        final String ddl = installer.buildCreateTableDDL(model);

        assertTrue(ddl.contains("`greptime_ts` TIMESTAMP TIME INDEX"));
        assertFalse(ddl.contains("DEFAULT CURRENT_TIMESTAMP()"),
            "Time-series model should not have DEFAULT timestamp");
    }

    // ---- DDL: Record model ----

    @Test
    void buildDDLForRecordShouldContainAppendMode() {
        final Model model = TestModels.sampleRecordModel();
        final String ddl = installer.buildCreateTableDDL(model);

        assertTrue(ddl.contains("CREATE TABLE IF NOT EXISTS segment"),
            "Record model should use plain name");
        assertTrue(ddl.contains("`id` STRING"), "Should include synthetic id column");
        assertTrue(ddl.contains("'append_mode' = 'true'"), "Records should use append_mode");
        assertTrue(ddl.contains("'ttl' = '3d'"), "Should have records TTL");
        assertFalse(ddl.contains("merge_mode"), "Records should not use merge_mode");
    }

    @Test
    void buildDDLForRecordShouldHaveServiceIdAsPrimaryKey() {
        final Model model = TestModels.sampleRecordModel();
        final String ddl = installer.buildCreateTableDDL(model);
        assertTrue(ddl.contains("PRIMARY KEY (`service_id`)"));
    }

    @Test
    void buildDDLForRecordShouldUseSkippingIndexForTraceId() {
        final Model model = TestModels.sampleRecordModel();
        final String ddl = installer.buildCreateTableDDL(model);
        assertTrue(ddl.contains("`trace_id` STRING SKIPPING INDEX"),
            "trace_id should have SKIPPING INDEX");
    }

    @Test
    void buildDDLForRecordShouldMapTagsAsJson() {
        final Model model = TestModels.sampleRecordModel();
        final String ddl = installer.buildCreateTableDDL(model);
        assertTrue(ddl.contains("`tags` JSON"), "List columns should map to JSON");
    }

    @Test
    void buildDDLForRecordShouldMapDataBinaryAsBinary() {
        final Model model = TestModels.sampleRecordModel();
        final String ddl = installer.buildCreateTableDDL(model);
        assertTrue(ddl.contains("`data_binary` BINARY"), "byte[] columns should map to BINARY");
    }

    // ---- DDL: Management model ----

    @Test
    void buildDDLForManagementShouldHaveIdAsPrimaryKey() {
        final Model model = TestModels.sampleManagementModel();
        final String ddl = installer.buildCreateTableDDL(model);

        assertTrue(ddl.contains("CREATE TABLE IF NOT EXISTS ui_template"));
        assertTrue(ddl.contains("PRIMARY KEY (`id`)"),
            "Management model should use id as PRIMARY KEY");
        assertTrue(ddl.contains("'merge_mode' = 'last_row'"));
    }

    @Test
    void buildDDLForManagementShouldHaveDefaultTimestamp() {
        final Model model = TestModels.sampleManagementModel();
        final String ddl = installer.buildCreateTableDDL(model);

        assertTrue(ddl.contains("`greptime_ts` TIMESTAMP DEFAULT CURRENT_TIMESTAMP() TIME INDEX"),
            "Non-timeSeries model should have DEFAULT CURRENT_TIMESTAMP()");
    }

    @Test
    void buildDDLForManagementShouldNotHaveTTLWhenZero() {
        final Model model = TestModels.sampleManagementModel();
        final String ddl = installer.buildCreateTableDDL(model);
        assertFalse(ddl.contains("'ttl'"), "Management with TTL=0 should not have TTL option");
    }

    @Test
    void buildDDLForManagementShouldHaveTTLWhenConfigured() {
        config.setManagementTTL("30d");
        final Model model = TestModels.sampleManagementModel();
        final String ddl = installer.buildCreateTableDDL(model);
        assertTrue(ddl.contains("'ttl' = '30d'"));
    }

    // ---- DDL: NoneStream model ----

    @Test
    void buildDDLForNoneStreamShouldHaveIdAsPrimaryKey() {
        final List<ModelColumn> columns = new ArrayList<>();
        columns.add(TestModels.col("task_id", String.class));
        columns.add(TestModels.col("service_id", String.class));
        columns.add(TestModels.col("duration", int.class));
        final Model model = TestModels.noneStreamModel("profile_task", columns);
        final String ddl = installer.buildCreateTableDDL(model);

        assertTrue(ddl.contains("PRIMARY KEY (`id`)"),
            "NoneStream model should use id as PRIMARY KEY");
        assertTrue(ddl.contains("'merge_mode' = 'last_row'"));
    }

    // ---- DDL: Metrics with different downsampling ----

    @Test
    void buildDDLForHourDownsamplingShouldUseHourSuffix() {
        final List<ModelColumn> columns = new ArrayList<>();
        columns.add(TestModels.col("service_id", String.class));
        columns.add(TestModels.col("value", long.class, true, 0));
        final Model model = TestModels.metricsModel("service_resp_time", DownSampling.Hour, columns);
        final String ddl = installer.buildCreateTableDDL(model);
        assertTrue(ddl.contains("CREATE TABLE IF NOT EXISTS service_resp_time_hour"));
    }

    // ---- selectPrimaryKeyColumns (delegated, but verify installer method) ----

    @Test
    void selectPrimaryKeyColumnsShouldDelegateToConverter() {
        final Model model = TestModels.sampleMetricsModel();
        final List<String> pk = installer.selectPrimaryKeyColumns(model);
        // Should match GreptimeDBConverter.selectPrimaryKeyColumns
        final List<String> expected = GreptimeDBConverter.selectPrimaryKeyColumns(model);
        assertEquals(expected, pk);
    }

    private void assertEquals(final List<String> expected, final List<String> actual) {
        org.junit.jupiter.api.Assertions.assertEquals(expected, actual);
    }
}
