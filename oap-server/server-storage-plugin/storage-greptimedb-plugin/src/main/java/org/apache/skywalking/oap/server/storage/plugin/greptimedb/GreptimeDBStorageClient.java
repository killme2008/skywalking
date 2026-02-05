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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.greptime.GreptimeDB;
import io.greptime.models.AuthInfo;
import io.greptime.models.Err;
import io.greptime.models.Result;
import io.greptime.models.Table;
import io.greptime.models.WriteOk;
import io.greptime.options.GreptimeOptions;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.oap.server.library.client.Client;

@Slf4j
public class GreptimeDBStorageClient implements Client {
    private final GreptimeDBStorageConfig config;

    @Getter
    private GreptimeDB grpcClient;
    @Getter
    private HikariDataSource jdbcDataSource;

    public GreptimeDBStorageClient(final GreptimeDBStorageConfig config) {
        this.config = config;
    }

    @Override
    public void connect() throws Exception {
        // gRPC client for writes
        final String[] endpoints = config.getGrpcEndpoints().split(",");
        final GreptimeOptions.Builder builder = GreptimeOptions.newBuilder(endpoints, config.getDatabase())
            .writeMaxRetries(3);
        if (!config.getUser().isEmpty()) {
            builder.authInfo(new AuthInfo(config.getUser(), config.getPassword()));
        }
        this.grpcClient = GreptimeDB.create(builder.build());
        log.info("GreptimeDB gRPC client connected to: {}", config.getGrpcEndpoints());

        // JDBC pool for reads + DDL
        final HikariConfig hikari = new HikariConfig();
        hikari.setJdbcUrl("jdbc:mysql://" + config.getJdbcHost() + ":" +
            config.getJdbcPort() + "/" + config.getDatabase());
        if (!config.getUser().isEmpty()) {
            hikari.setUsername(config.getUser());
            hikari.setPassword(config.getPassword());
        }
        hikari.setMaximumPoolSize(config.getMaxJdbcPoolSize());
        this.jdbcDataSource = new HikariDataSource(hikari);
        log.info("GreptimeDB JDBC pool connected to: {}:{}", config.getJdbcHost(), config.getJdbcPort());
    }

    public CompletableFuture<Result<WriteOk, Err>> write(final Table... tables) {
        return grpcClient.write(tables);
    }

    public Connection getConnection() throws SQLException {
        return jdbcDataSource.getConnection();
    }

    public void executeDDL(final String sql) throws SQLException {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    @Override
    public void shutdown() throws IOException {
        if (grpcClient != null) {
            try {
                grpcClient.shutdownGracefully();
            } catch (Exception e) {
                log.warn("Failed to shutdown gRPC client", e);
            }
        }
        if (jdbcDataSource != null) {
            jdbcDataSource.close();
        }
    }
}
