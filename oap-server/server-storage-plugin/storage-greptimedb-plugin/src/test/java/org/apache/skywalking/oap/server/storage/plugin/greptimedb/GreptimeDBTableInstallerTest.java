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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.skywalking.oap.server.core.analysis.DownSampling;
import org.apache.skywalking.oap.server.core.storage.StorageException;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.core.storage.model.ModelColumn;
import org.apache.skywalking.oap.server.core.storage.model.StorageManipulationOpt;
import org.apache.skywalking.oap.server.library.module.ModuleManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GreptimeDBTableInstallerTest {

    @Mock
    private GreptimeDBStorageClient client;

    private ModuleManager moduleManager;
    private GreptimeDBStorageConfig config;
    private GreptimeDBTableInstaller installer;

    @BeforeEach
    void setUp() {
        config = new GreptimeDBStorageConfig();
        config.setMetricsTTL("7d");
        config.setRecordsTTL("3d");
        moduleManager = TestModels.mockModuleManager(Collections.emptySet(), "", "");
        installer = new GreptimeDBTableInstaller(client, moduleManager, config, new SchemaRegistry(config));
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
        assertTrue(ddl.contains("PRIMARY KEY (`entity_id`)"),
            "Should have entity_id as PRIMARY KEY");
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
    void buildDDLForRecordShouldNotUseSkippingIndexForRangeColumns() {
        final Model model = TestModels.sampleRecordModel();
        final String ddl = installer.buildCreateTableDDL(model);
        assertTrue(ddl.contains("`latency` INT"));
        assertFalse(ddl.contains("`latency` INT SKIPPING INDEX"));
    }

    @Test
    void buildDDLForRecordShouldNotStoreTagsAsJsonWhenNoSearchableTags() {
        final Model model = TestModels.sampleRecordModel();
        final String ddl = installer.buildCreateTableDDL(model);
        assertFalse(ddl.contains("`tags`"), "Record tags belong to the normalized additional table");
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
    void buildDDLForManagementShouldNeverExpire() {
        final Model model = TestModels.sampleManagementModel();
        final String ddl = installer.buildCreateTableDDL(model);
        // Config tables use a constant greptime_ts; 'forever' keeps them from ever being purged.
        assertTrue(ddl.contains("'ttl' = 'forever'"),
            "Management (non-timeSeries) tables must be created with ttl = 'forever'");
    }

    // ---- DDL: NoneStream model ----

    @Test
    void buildDDLForNoneStreamShouldUseRecordStorageSemantics() {
        final List<ModelColumn> columns = new ArrayList<>();
        columns.add(TestModels.col("task_id", String.class));
        columns.add(TestModels.col("service_id", String.class));
        columns.add(TestModels.col("duration", int.class));
        final Model model = TestModels.noneStreamModel("profile_task", columns);
        final String ddl = installer.buildCreateTableDDL(model);

        assertTrue(ddl.contains("PRIMARY KEY (`service_id`)"));
        assertTrue(ddl.contains("'append_mode' = 'true'"));
        assertTrue(ddl.contains("'ttl' = '3d'"));
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

    @Test
    void loadsSchemaSnapshotOnceAndTracksCreatedTables() throws Exception {
        final Connection connection = mock(Connection.class);
        final PreparedStatement statement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);
        when(client.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        final Model metrics = TestModels.sampleMetricsModel();
        assertFalse(installer.isExists(metrics, StorageManipulationOpt.schemaCreateIfAbsent()).isAllExist());

        installer.createTable(metrics);

        assertTrue(installer.isExists(metrics, StorageManipulationOpt.schemaCreateIfAbsent()).isAllExist());
        assertFalse(installer.isExists(
            TestModels.sampleRecordModel(), StorageManipulationOpt.schemaCreateIfAbsent()).isAllExist());
        verify(client, times(1)).getConnection();
        verify(statement, times(3)).executeQuery();
    }

    @Test
    void isExistsMatchesLowerCasedTableForCamelCaseModel() throws Exception {
        // Regression: GreptimeDB folds an unquoted table name to lower case, so a camelCase model
        // (rocketmq's commitLog) lands in a physical table whose information_schema name is lower
        // case. isExists must resolve the model to that same lower-cased name; otherwise the
        // case-sensitive snapshot lookup reports MISSING and a no-init OAP waits forever. Feed a
        // cold snapshot (from information_schema, NOT an in-process createTable) that holds only the
        // lower-cased table with no columns: once the name resolves, the shape diff trips and
        // surfaces the canonical lower-case name — proving the table was matched by it.
        final Connection connection = mock(Connection.class);
        final PreparedStatement statement = mock(PreparedStatement.class);
        final ResultSet tables = mock(ResultSet.class);
        final ResultSet empty = mock(ResultSet.class);
        when(client.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(contains("information_schema.tables"))).thenReturn(statement);
        when(connection.prepareStatement(contains("information_schema.columns"))).thenReturn(statement);
        when(connection.prepareStatement(contains("information_schema.statistics"))).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(tables, empty, empty);
        when(tables.next()).thenReturn(true, false);
        when(tables.getString(1)).thenReturn("meter_rocketmq_cluster_max_commitlog_disk_ratio_minute");
        when(tables.getString(2)).thenReturn("");
        when(empty.next()).thenReturn(false);

        final List<ModelColumn> columns = new ArrayList<>();
        columns.add(TestModels.col("service_id", String.class));
        columns.add(TestModels.col("value", long.class, true, 0));
        final Model model = TestModels.metricsModel(
            "meter_rocketmq_cluster_max_commitLog_disk_ratio", DownSampling.Minute, columns);

        final StorageException thrown = assertThrows(StorageException.class,
            () -> installer.isExists(model, StorageManipulationOpt.schemaCreateIfAbsent()));
        assertTrue(thrown.getMessage().contains(
                "meter_rocketmq_cluster_max_commitlog_disk_ratio_minute"),
            "table must be resolved and reported by its lower-cased canonical name");
    }

    @Test
    void fallsBackToKeyColumnUsageWhenStatisticsViewIsMissing() throws Exception {
        final Connection connection = mock(Connection.class);
        final PreparedStatement tablesStatement = mock(PreparedStatement.class);
        final PreparedStatement columnsStatement = mock(PreparedStatement.class);
        final PreparedStatement indexesStatement = mock(PreparedStatement.class);
        final PreparedStatement legacyIndexesStatement = mock(PreparedStatement.class);
        final ResultSet tables = mock(ResultSet.class);
        final ResultSet columns = mock(ResultSet.class);
        final ResultSet indexes = mock(ResultSet.class);
        when(client.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(contains("information_schema.tables")))
            .thenReturn(tablesStatement);
        when(connection.prepareStatement(contains("information_schema.columns")))
            .thenReturn(columnsStatement);
        when(connection.prepareStatement(contains("information_schema.statistics")))
            .thenReturn(indexesStatement);
        when(connection.prepareStatement(contains("information_schema.key_column_usage")))
            .thenReturn(legacyIndexesStatement);
        when(tablesStatement.executeQuery()).thenReturn(tables);
        when(columnsStatement.executeQuery()).thenReturn(columns);
        when(indexesStatement.executeQuery()).thenThrow(new SQLException(
            "Table not found: greptime.information_schema.statistics"));
        when(legacyIndexesStatement.executeQuery()).thenReturn(indexes);
        when(tables.next()).thenReturn(true, false);
        when(tables.getString(1)).thenReturn("existing_table");
        when(tables.getString(2)).thenReturn("");
        when(columns.next()).thenReturn(false);
        when(indexes.next()).thenReturn(false);

        assertFalse(installer.isExists(
            TestModels.sampleMetricsModel(), StorageManipulationOpt.schemaCreateIfAbsent()).isAllExist());

        verify(legacyIndexesStatement).executeQuery();
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
