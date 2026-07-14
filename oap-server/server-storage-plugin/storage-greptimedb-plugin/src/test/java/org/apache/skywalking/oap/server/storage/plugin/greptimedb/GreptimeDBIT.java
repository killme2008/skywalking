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

import io.greptime.GreptimeDB;
import io.greptime.models.Err;
import io.greptime.models.Result;
import io.greptime.models.Table;
import io.greptime.models.WriteOk;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.oap.server.core.analysis.DownSampling;
import org.apache.skywalking.oap.server.core.analysis.Layer;
import org.apache.skywalking.oap.server.core.analysis.manual.service.ServiceTraffic;
import org.apache.skywalking.oap.server.core.management.ui.template.UITemplate;
import org.apache.skywalking.oap.server.core.profiling.continuous.storage.ContinuousProfilingPolicy;
import org.apache.skywalking.oap.server.core.query.PointOfTime;
import org.apache.skywalking.oap.server.core.query.enumeration.Order;
import org.apache.skywalking.oap.server.core.query.enumeration.Scope;
import org.apache.skywalking.oap.server.core.query.enumeration.Step;
import org.apache.skywalking.oap.server.core.query.input.DashboardSetting;
import org.apache.skywalking.oap.server.core.query.input.Duration;
import org.apache.skywalking.oap.server.core.query.input.Entity;
import org.apache.skywalking.oap.server.core.query.input.MetricsCondition;
import org.apache.skywalking.oap.server.core.query.type.KVInt;
import org.apache.skywalking.oap.server.core.query.type.Logs;
import org.apache.skywalking.oap.server.core.query.type.MetricsValues;
import org.apache.skywalking.oap.server.core.query.type.Service;
import org.apache.skywalking.oap.server.core.storage.annotation.Column;
import org.apache.skywalking.oap.server.core.storage.annotation.ValueColumnMetadata;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.core.storage.model.ModelColumn;
import org.apache.skywalking.oap.server.core.storage.model.StorageManipulationOpt;
import org.apache.skywalking.oap.server.library.module.ModuleManager;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBContinuousProfilingPolicyDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBLogQueryDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBManagementDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBMetadataQueryDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBMetricsQueryDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBTableBuilder;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBUITemplateManagementDAO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@Testcontainers
class GreptimeDBIT {
    private static final int HTTP_PORT = 4000;
    private static final int GRPC_PORT = 4001;
    private static final int MYSQL_PORT = 4002;

    @Container
    public GenericContainer<?> greptimeDB = new GenericContainer<>(
        DockerImageName.parse("greptime/greptimedb:v1.1.2"))
        .withCommand("standalone", "start",
            "--http-addr", "0.0.0.0:4000",
            "--rpc-bind-addr", "0.0.0.0:4001",
            "--mysql-addr", "0.0.0.0:4002")
        .withExposedPorts(HTTP_PORT, GRPC_PORT, MYSQL_PORT)
        .waitingFor(Wait.forHttp("/health").forPort(HTTP_PORT));

    private GreptimeDBStorageClient client;
    private GreptimeDBStorageConfig config;
    private GreptimeDBTableInstaller installer;
    private GreptimeDBSearchableTagColumns tagColumns;

    @BeforeEach
    void setUp() throws Exception {
        config = new GreptimeDBStorageConfig();
        config.setGrpcEndpoints(greptimeDB.getHost() + ":" + greptimeDB.getMappedPort(GRPC_PORT));
        config.setJdbcHost(greptimeDB.getHost());
        config.setJdbcPort(greptimeDB.getMappedPort(MYSQL_PORT));
        config.setDatabase("public");
        config.setMetricsTTL("7d");
        config.setRecordsTTL("3d");

        final ModuleManager moduleManager = TestModels.mockModuleManager(
            Set.of("http.method", "status_code", "db.type"), "", "");
        tagColumns = new GreptimeDBSearchableTagColumns(moduleManager, config);
        client = new GreptimeDBStorageClient(config);
        client.connect();
        installer = new GreptimeDBTableInstaller(client, moduleManager, config);
    }

    @Test
    void testCreateMetricsTable() throws Exception {
        final Model model = TestModels.sampleMetricsModel();
        installer.createTable(model);

        try (Connection conn = client.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW TABLES LIKE 'service_resp_time_minute'")) {
            assertTrue(rs.next(), "Metrics table should exist after creation");
        }

        // Verify table structure by describing it
        try (Connection conn = client.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("DESC service_resp_time_minute")) {
            boolean hasId = false;
            boolean hasServiceId = false;
            boolean hasGreptimeTs = false;
            while (rs.next()) {
                final String colName = rs.getString("Column");
                if ("id".equals(colName)) {
                    hasId = true;
                } else if ("service_id".equals(colName)) {
                    hasServiceId = true;
                } else if ("greptime_ts".equals(colName)) {
                    hasGreptimeTs = true;
                }
            }
            assertTrue(hasId, "Should have synthetic id column");
            assertTrue(hasServiceId, "Should have service_id column");
            assertTrue(hasGreptimeTs, "Should have greptime_ts TIME INDEX column");
        }
    }

    @Test
    void testCreateRecordTable() throws Exception {
        final Model model = TestModels.sampleRecordModel();
        installer.createTable(model);

        try (Connection conn = client.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW TABLES LIKE 'segment'")) {
            assertTrue(rs.next(), "Record table should exist after creation");
        }
    }

    @Test
    void testCreateManagementTable() throws Exception {
        final Model model = TestModels.sampleManagementModel();
        installer.createTable(model);

        try (Connection conn = client.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW TABLES LIKE 'ui_template'")) {
            assertTrue(rs.next(), "Management table should exist after creation");
        }
    }

    @Test
    void testIdempotentTableCreation() throws Exception {
        final Model model = TestModels.sampleMetricsModel();
        // Create twice should not fail (CREATE TABLE IF NOT EXISTS)
        installer.createTable(model);
        installer.createTable(model);

        try (Connection conn = client.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW TABLES LIKE 'service_resp_time_minute'")) {
            assertTrue(rs.next());
        }
    }

    @Test
    void testWriteAndReadMetrics() throws Exception {
        final Model model = TestModels.sampleMetricsModel();
        installer.createTable(model);

        // Build and write a single row via gRPC
        final SchemaRegistry registry = new SchemaRegistry();
        final SchemaRegistry.WriteSchemaInfo schemaInfo = registry.getWriteSchema(model);
        final Table table = Table.from(schemaInfo.getTableSchema());
        final long ts = System.currentTimeMillis();
        // Column order: id, service_id, entity_id, time_bucket, summation, count, value, greptime_ts
        table.addRow("test-id-001", "svc1", "svc1_entity", 202401011200L, 500L, 5L, 100L, ts);

        final GreptimeDB grpcClient = client.getGrpcClient();
        final CompletableFuture<Result<WriteOk, Err>> future = grpcClient.write(table);
        final Result<WriteOk, Err> result = future.get(10, TimeUnit.SECONDS);
        assertTrue(result.isOk(), "gRPC write should succeed: " + result);

        // Query via JDBC
        try (Connection conn = client.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT id, service_id, entity_id, summation, count, value " +
                 "FROM service_resp_time_minute WHERE service_id = 'svc1'")) {
            assertTrue(rs.next(), "Should read back the written row");
            assertEquals("test-id-001", rs.getString("id"));
            assertEquals("svc1", rs.getString("service_id"));
            assertEquals("svc1_entity", rs.getString("entity_id"));
            assertEquals(500L, rs.getLong("summation"));
            assertEquals(5L, rs.getLong("count"));
            assertEquals(100L, rs.getLong("value"));
        }
    }

    @Test
    void testMergeModeUpsert() throws Exception {
        final Model model = TestModels.sampleMetricsModel();
        installer.createTable(model);

        final SchemaRegistry registry = new SchemaRegistry();
        final SchemaRegistry.WriteSchemaInfo schemaInfo = registry.getWriteSchema(model);
        final long ts = System.currentTimeMillis();

        // Write initial row
        final Table table1 = Table.from(schemaInfo.getTableSchema());
        table1.addRow("upsert-id", "svc1", "svc1_entity", 202401011200L, 100L, 1L, 100L, ts);
        Result<WriteOk, Err> r1 = client.getGrpcClient().write(table1).get(10, TimeUnit.SECONDS);
        assertTrue(r1.isOk(), "First write should succeed");

        // Write same primary key + timestamp with updated values (merge_mode=last_row should upsert)
        final Table table2 = Table.from(schemaInfo.getTableSchema());
        table2.addRow("upsert-id", "svc1", "svc1_entity", 202401011200L, 200L, 2L, 100L, ts);
        Result<WriteOk, Err> r2 = client.getGrpcClient().write(table2).get(10, TimeUnit.SECONDS);
        assertTrue(r2.isOk(), "Second write (upsert) should succeed");

        // Should have only 1 row with updated values
        try (Connection conn = client.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT summation, count FROM service_resp_time_minute " +
                 "WHERE service_id = 'svc1' AND entity_id = 'svc1_entity'")) {
            assertTrue(rs.next(), "Should find the upserted row");
            assertEquals(200L, rs.getLong("summation"), "Summation should be updated to latest value");
            assertEquals(2L, rs.getLong("count"), "Count should be updated to latest value");
        }
    }

    @Test
    void testIsExistsForNonExistentTable() throws Exception {
        final Model model = TestModels.sampleMetricsModel();
        final GreptimeDBTableInstaller.InstallInfo info =
            installer.isExists(model, StorageManipulationOpt.schemaCreateIfAbsent());
        assertTrue(!info.isAllExist(), "Non-existent table should return false");
    }

    @Test
    void testIsExistsForExistingTable() throws Exception {
        final Model model = TestModels.sampleMetricsModel();
        installer.createTable(model);
        final GreptimeDBTableInstaller.InstallInfo info =
            installer.isExists(model, StorageManipulationOpt.schemaCreateIfAbsent());
        assertTrue(info.isAllExist(), "Existing table should return true");
    }

    // ---- management/config tables must upsert in place, not append versioned rows ----

    @Test
    void testUITemplateChangeUpsertsInPlace() throws Exception {
        installer.createTable(uiTemplateModel());
        final GreptimeDBUITemplateManagementDAO dao = new GreptimeDBUITemplateManagementDAO(client);

        final DashboardSetting setting = new DashboardSetting();
        setting.setId("dashboard-1");
        setting.setConfiguration("v1");
        dao.addTemplate(setting);

        // Changing the same template must overwrite in place, not append a second row.
        setting.setConfiguration("v2");
        dao.changeTemplate(setting);

        assertEquals(1, dao.getAllTemplates(true).size(),
            "changeTemplate must upsert, not append a second versioned row");
        assertEquals("v2", dao.getTemplate("dashboard-1").getConfiguration(),
            "getTemplate must return the latest configuration");

        // disableTemplate must also upsert in place and drop out of the enabled-only listing.
        dao.disableTemplate("dashboard-1");
        assertEquals(1, dao.getAllTemplates(true).size(), "disable must not create a new row");
        assertTrue(dao.getAllTemplates(false).isEmpty(),
            "disabled template must be excluded when includingDisabled=false");
    }

    @Test
    void testContinuousProfilingPolicyUpsertsInPlace() throws Exception {
        installer.createTable(continuousProfilingPolicyModel());
        final GreptimeDBContinuousProfilingPolicyDAO dao = new GreptimeDBContinuousProfilingPolicyDAO(client);

        final ContinuousProfilingPolicy p1 = new ContinuousProfilingPolicy();
        p1.setServiceId("svc-1");
        p1.setUuid("uuid-1");
        p1.setConfigurationJson("{\"v\":1}");
        dao.savePolicy(p1);

        final ContinuousProfilingPolicy p2 = new ContinuousProfilingPolicy();
        p2.setServiceId("svc-1");
        p2.setUuid("uuid-2");
        p2.setConfigurationJson("{\"v\":2}");
        dao.savePolicy(p2);

        final List<ContinuousProfilingPolicy> policies =
            dao.queryPolicies(Collections.singletonList("svc-1"));
        assertEquals(1, policies.size(), "savePolicy must upsert per serviceId, not append");
        assertEquals("uuid-2", policies.get(0).getUuid(), "queryPolicies must return the latest policy");
    }

    @Test
    void testManagementDAOGrpcUpsertsInPlace() throws Exception {
        final Model model = uiTemplateModel();
        installer.createTable(model);
        final GreptimeDBManagementDAO dao =
            new GreptimeDBManagementDAO(client, new SchemaRegistry(), new UITemplate.Builder());

        final UITemplate t1 = new UITemplate();
        t1.setTemplateId("g1");
        t1.setConfiguration("v1");
        t1.setUpdateTime(1L);
        t1.setDisabled(0);
        dao.insert(model, t1);

        final UITemplate t2 = new UITemplate();
        t2.setTemplateId("g1");
        t2.setConfiguration("v2");
        t2.setUpdateTime(2L);
        t2.setDisabled(0);
        dao.insert(model, t2);

        try (Connection conn = client.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT configuration FROM ui_template WHERE template_id = 'g1'")) {
            assertTrue(rs.next(), "row must exist after gRPC insert");
            assertEquals("v2", rs.getString("configuration"),
                "gRPC insert must upsert to the latest value via constant greptime_ts");
            assertFalse(rs.next(), "constant greptime_ts must keep exactly one row per entity");
        }
    }

    private Model uiTemplateModel() {
        final List<ModelColumn> cols = new ArrayList<>();
        cols.add(TestModels.col("template_id", String.class));
        cols.add(TestModels.col("configuration", String.class, true, 1_000_000));
        cols.add(TestModels.col("update_time", long.class));
        cols.add(TestModels.col("disabled", int.class));
        return TestModels.managementModel("ui_template", cols);
    }

    private Model continuousProfilingPolicyModel() {
        final List<ModelColumn> cols = new ArrayList<>();
        cols.add(TestModels.col("service_id", String.class));
        cols.add(TestModels.col("uuid", String.class));
        cols.add(TestModels.col("configuration_json", String.class, true, 5000));
        return TestModels.managementModel("continuous_profiling_policy", cols);
    }

    // ---- traffic/metadata reads must return the latest row per entity, not one per active minute ----

    @Test
    void testListServicesDedupsPerMinuteRows() throws Exception {
        final Model model = serviceTrafficModel();
        installer.createTable(model);
        final SchemaRegistry registry = new SchemaRegistry();
        final SchemaRegistry.WriteSchemaInfo schemaInfo = registry.getWriteSchema(model);

        final ServiceTraffic svc = new ServiceTraffic();
        svc.setName("serviceA");
        svc.setServiceId("serviceA-id");
        svc.setLayer(Layer.GENERAL);

        // Same entity persisted in two different minutes: stable id, increasing greptime_ts -> two rows.
        for (final long ts : new long[] {1_704_067_200_000L, 1_704_067_260_000L}) {
            final Table table = GreptimeDBTableBuilder.buildTable(
                svc, new ServiceTraffic.Builder(), model, schemaInfo, ts);
            final Result<WriteOk, Err> r = client.getGrpcClient().write(table).get(10, TimeUnit.SECONDS);
            assertTrue(r.isOk(), "traffic write should succeed: " + r);
        }

        final GreptimeDBMetadataQueryDAO dao = new GreptimeDBMetadataQueryDAO(client, 5000);
        final List<Service> services = dao.listServices();
        assertEquals(1, services.size(), "listServices must collapse the per-minute rows to one service");
        assertEquals("serviceA", services.get(0).getName());
    }

    private Model serviceTrafficModel() {
        final List<ModelColumn> cols = new ArrayList<>();
        cols.add(TestModels.col("service_traffic_name", String.class));
        cols.add(TestModels.col("short_name", String.class));
        cols.add(TestModels.col("service_id", String.class));
        cols.add(TestModels.col("service_group", String.class));
        cols.add(TestModels.col("layer", Layer.class));
        cols.add(TestModels.col("time_bucket", long.class));
        return TestModels.metricsModel("service_traffic", DownSampling.Minute, cols);
    }

    // ---- entity-scoped metrics reads add entity_id/greptime_ts pruning without dropping rows ----

    @Test
    void testReadMetricsValuesReturnsAllPointsWithPrunedPredicates() throws Exception {
        final Model model = serviceRespTimeModel();
        installer.createTable(model);
        final SchemaRegistry registry = new SchemaRegistry();
        final SchemaRegistry.WriteSchemaInfo schemaInfo = registry.getWriteSchema(model);

        final Entity entity = new Entity();
        entity.setScope(Scope.Service);
        entity.setServiceName("svcB3");
        entity.setNormal(true);
        final String entityId = entity.buildId();

        final Duration duration = new Duration();
        duration.setStep(Step.MINUTE);
        duration.setStart("2024-01-01 0000");
        duration.setEnd("2024-01-01 0002");

        final List<PointOfTime> points = duration.assembleDurationPoints();
        long value = 100L;
        for (final PointOfTime point : points) {
            final long bucket = point.getPoint();
            final Table table = Table.from(schemaInfo.getTableSchema());
            // Column order: id, entity_id, value, time_bucket, greptime_ts
            table.addRow(point.id(entityId), entityId, value, bucket,
                GreptimeDBConverter.timeBucketToTimestamp(bucket, DownSampling.Minute));
            final Result<WriteOk, Err> r = client.getGrpcClient().write(table).get(10, TimeUnit.SECONDS);
            assertTrue(r.isOk(), "metric write should succeed: " + r);
            value += 100L;
        }

        // readMetricsValues resolves the default through ValueColumnMetadata; register the metric first.
        ValueColumnMetadata.INSTANCE.putIfAbsent(
            "service_resp_time", "value", Column.ValueDataType.COMMON_VALUE, 0, 0);

        final MetricsCondition condition = new MetricsCondition();
        condition.setName("service_resp_time");
        condition.setEntity(entity);

        final GreptimeDBMetricsQueryDAO dao = new GreptimeDBMetricsQueryDAO(client);
        final MetricsValues values = dao.readMetricsValues(condition, "value", duration);

        final List<KVInt> kvs = values.getValues().getValues();
        assertEquals(points.size(), kvs.size(),
            "every in-range point must be returned despite the added pruning predicates");
        for (final KVInt kv : kvs) {
            assertTrue(kv.getValue() > 0,
                "a written point must keep its value, not the default that a wrongly-pruned row would leave");
        }
    }

    private Model serviceRespTimeModel() {
        final List<ModelColumn> cols = new ArrayList<>();
        cols.add(TestModels.col("entity_id", String.class));
        cols.add(TestModels.col("value", long.class, true, 0));
        cols.add(TestModels.col("time_bucket", long.class));
        return TestModels.metricsModel("service_resp_time", DownSampling.Minute, cols);
    }

    // ---- log content keyword search via matches_term over the FULLTEXT-indexed content column ----

    @Test
    void testQueryLogsByContentKeyword() throws Exception {
        final Model model = logRecordModel();
        installer.createTable(model);
        final SchemaRegistry registry = new SchemaRegistry();
        final SchemaRegistry.WriteSchemaInfo schemaInfo = registry.getWriteSchema(model);

        writeLogRow(schemaInfo, "log-1", "user login failed with ERROR", 1000L);
        writeLogRow(schemaInfo, "log-2", "GET api users returned ok", 2000L);
        writeLogRow(schemaInfo, "log-3", "database connection timeout", 3000L);

        final GreptimeDBLogQueryDAO dao = new GreptimeDBLogQueryDAO(client, tagColumns);

        // include 'error' — case-insensitive matches_term must hit the uppercase ERROR row only.
        final Logs included = dao.queryLogs(null, null, null, null, Order.DES, 0, 10, null, null,
            Collections.singletonList("error"), null);
        assertEquals(1, included.getLogs().size(), "keyword 'error' must match exactly one log");
        assertTrue(included.getLogs().get(0).getContent().contains("ERROR"));

        // exclude 'error' — the other two logs come back.
        final Logs excluded = dao.queryLogs(null, null, null, null, Order.DES, 0, 10, null, null,
            null, Collections.singletonList("error"));
        assertEquals(2, excluded.getLogs().size(), "excluding 'error' must drop only the ERROR log");
    }

    private void writeLogRow(final SchemaRegistry.WriteSchemaInfo schemaInfo, final String id,
                             final String content, final long ts) throws Exception {
        final Table table = Table.from(schemaInfo.getTableSchema());
        // Column order: id, service_id, service_instance_id, endpoint_id, trace_id, content_type,
        // content, tags_raw_data, timestamp, time_bucket, greptime_ts
        table.addRow(id, "svc", "inst", null, "trace-1", 1, content, new byte[0], ts, 202401010000L, ts);
        final Result<WriteOk, Err> r = client.getGrpcClient().write(table).get(10, TimeUnit.SECONDS);
        assertTrue(r.isOk(), "log write should succeed: " + r);
    }

    private Model logRecordModel() {
        final List<ModelColumn> cols = new ArrayList<>();
        cols.add(TestModels.col("service_id", String.class));
        cols.add(TestModels.col("service_instance_id", String.class));
        cols.add(TestModels.col("endpoint_id", String.class));
        cols.add(TestModels.col("trace_id", String.class));
        cols.add(TestModels.col("content_type", int.class));
        cols.add(TestModels.col("content", String.class, false, 1_000_000));
        cols.add(TestModels.col("tags_raw_data", byte[].class, true, 0));
        cols.add(TestModels.col("timestamp", long.class));
        cols.add(TestModels.col("time_bucket", long.class));
        return TestModels.recordModel("log", cols);
    }
}
