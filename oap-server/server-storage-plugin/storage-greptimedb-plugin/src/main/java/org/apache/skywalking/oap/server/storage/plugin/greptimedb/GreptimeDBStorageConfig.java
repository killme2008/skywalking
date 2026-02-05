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

import lombok.Getter;
import lombok.Setter;
import org.apache.skywalking.oap.server.library.module.ModuleConfig;

@Getter
@Setter
public class GreptimeDBStorageConfig extends ModuleConfig {
    /**
     * gRPC write endpoint (GreptimeDB port 4001). Comma-separated for multiple endpoints.
     */
    private String grpcEndpoints = "127.0.0.1:4001";

    /**
     * JDBC read endpoint host (GreptimeDB MySQL protocol, port 4002).
     */
    private String jdbcHost = "127.0.0.1";

    /**
     * JDBC read endpoint port.
     */
    private int jdbcPort = 4002;

    /**
     * Database name.
     */
    private String database = "skywalking";

    /**
     * Authentication user.
     */
    private String user = "";

    /**
     * Authentication password.
     */
    private String password = "";

    /**
     * TTL for metrics data (e.g., "7d", "168h").
     */
    private String metricsTTL = "7d";

    /**
     * TTL for record data (traces, logs, alarms).
     */
    private String recordsTTL = "3d";

    /**
     * TTL for management data. "0" means no expiry.
     */
    private String managementTTL = "0";

    /**
     * Max number of rows per bulk write.
     */
    private int maxBulkSize = 5000;

    /**
     * Flush interval in seconds.
     */
    private int flushInterval = 15;

    /**
     * Max JDBC connection pool size.
     */
    private int maxJdbcPoolSize = 10;

    /**
     * Max number of rows returned for metadata queries (services, instances, endpoints).
     */
    private int metadataQueryMaxSize = 5000;
}
