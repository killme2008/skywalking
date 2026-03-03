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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.library.module.ModuleManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

@Slf4j
@Testcontainers
class GreptimeDBIT {
    private static final int HTTP_PORT = 4000;
    private static final int GRPC_PORT = 4001;
    private static final int MYSQL_PORT = 4002;

    @Container
    public GenericContainer<?> greptimeDB = new GenericContainer<>(
        DockerImageName.parse("greptime/greptimedb:v1.0.0-rc.1"))
        .withCommand("standalone", "start",
            "--http-addr", "0.0.0.0:4000",
            "--rpc-bind-addr", "0.0.0.0:4001",
            "--mysql-addr", "0.0.0.0:4002")
        .withExposedPorts(HTTP_PORT, GRPC_PORT, MYSQL_PORT)
        .waitingFor(Wait.forHttp("/health").forPort(HTTP_PORT));

    private GreptimeDBStorageClient client;
    private GreptimeDBStorageConfig config;
    private GreptimeDBTableInstaller installer;

    @BeforeEach
    void setUp() throws Exception {
        config = new GreptimeDBStorageConfig();
        config.setGrpcEndpoints(greptimeDB.getHost() + ":" + greptimeDB.getMappedPort(GRPC_PORT));
        config.setJdbcHost(greptimeDB.getHost());
        config.setJdbcPort(greptimeDB.getMappedPort(MYSQL_PORT));
        config.setDatabase("public");
        config.setMetricsTTL("7d");
        config.setRecordsTTL("3d");
        config.setManagementTTL("0");

        final ModuleManager moduleManager = mock(ModuleManager.class);
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
        final GreptimeDBTableInstaller.InstallInfo info = installer.isExists(model);
        assertTrue(!info.isAllExist(), "Non-existent table should return false");
    }

    @Test
    void testIsExistsForExistingTable() throws Exception {
        final Model model = TestModels.sampleMetricsModel();
        installer.createTable(model);
        final GreptimeDBTableInstaller.InstallInfo info = installer.isExists(model);
        assertTrue(info.isAllExist(), "Existing table should return true");
    }
}
