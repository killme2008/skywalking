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

import com.google.common.base.Strings;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.skywalking.oap.server.core.status.ConfigDumpExtension;

/**
 * Flattens the loaded, environment-resolved {@link GreptimeDBStorageConfig} into
 * {@code /debugging/config/dump} rows so a GreptimeDB deployment reports its effective storage
 * configuration like the other backends. Values are read straight from the POJO (post
 * environment-override); the {@code .password} key is masked centrally by the dump.
 */
public class GreptimeDBConfigDumpExtension implements ConfigDumpExtension {
    private final String prefix;
    private final GreptimeDBStorageConfig config;

    public GreptimeDBConfigDumpExtension(final String prefix, final GreptimeDBStorageConfig config) {
        this.prefix = prefix;
        this.config = config;
    }

    @Override
    public Map<String, String> dumpConfigurations() {
        final Map<String, String> dump = new LinkedHashMap<>();
        dump.put(prefix + ".grpcEndpoints", Strings.nullToEmpty(config.getGrpcEndpoints()));
        dump.put(prefix + ".jdbcHost", Strings.nullToEmpty(config.getJdbcHost()));
        dump.put(prefix + ".jdbcPort", String.valueOf(config.getJdbcPort()));
        dump.put(prefix + ".database", Strings.nullToEmpty(config.getDatabase()));
        dump.put(prefix + ".user", Strings.nullToEmpty(config.getUser()));
        dump.put(prefix + ".password", Strings.nullToEmpty(config.getPassword()));
        dump.put(prefix + ".metricsTTL", Strings.nullToEmpty(config.getMetricsTTL()));
        dump.put(prefix + ".recordsTTL", Strings.nullToEmpty(config.getRecordsTTL()));
        dump.put(prefix + ".maxJdbcPoolSize", String.valueOf(config.getMaxJdbcPoolSize()));
        dump.put(prefix + ".metadataQueryMaxSize", String.valueOf(config.getMetadataQueryMaxSize()));
        return dump;
    }
}
