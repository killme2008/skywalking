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

package org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.skywalking.oap.server.core.profiling.continuous.storage.ContinuousProfilingPolicy;
import org.apache.skywalking.oap.server.core.storage.profiling.continuous.IContinuousProfilingPolicyDAO;
import org.apache.skywalking.oap.server.library.util.CollectionUtils;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBConverter;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBStorageClient;

@RequiredArgsConstructor
public class GreptimeDBContinuousProfilingPolicyDAO implements IContinuousProfilingPolicyDAO {
    private final GreptimeDBStorageClient client;

    @Override
    public void savePolicy(final ContinuousProfilingPolicy policy) throws IOException {
        final String sql = "insert into " + ContinuousProfilingPolicy.INDEX_NAME
            + " (" + ContinuousProfilingPolicy.SERVICE_ID
            + ", " + ContinuousProfilingPolicy.UUID
            + ", " + ContinuousProfilingPolicy.CONFIGURATION_JSON + ") values (?, ?, ?)";
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, policy.getServiceId());
            ps.setString(2, policy.getUuid());
            ps.setString(3, policy.getConfigurationJson());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IOException("Failed to save continuous profiling policy", e);
        }
    }

    @Override
    public List<ContinuousProfilingPolicy> queryPolicies(
            final List<String> serviceIdList) throws IOException {
        if (CollectionUtils.isEmpty(serviceIdList)) {
            return new ArrayList<>();
        }
        final String placeholders = serviceIdList.stream().map(id -> "?")
            .collect(Collectors.joining(","));
        final String sql = "select * from " + ContinuousProfilingPolicy.INDEX_NAME
            + " where " + ContinuousProfilingPolicy.SERVICE_ID
            + " in (" + placeholders + ")";
        final List<ContinuousProfilingPolicy> policies = new ArrayList<>();
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < serviceIdList.size(); i++) {
                ps.setString(i + 1, serviceIdList.get(i));
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    final Map<String, Object> map = GreptimeDBConverter.resultSetToGenericMap(rs);
                    policies.add(new ContinuousProfilingPolicy.Builder()
                        .storage2Entity(new GreptimeDBConverter.ToEntity(map)));
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to query continuous profiling policies", e);
        }
        return policies;
    }
}
