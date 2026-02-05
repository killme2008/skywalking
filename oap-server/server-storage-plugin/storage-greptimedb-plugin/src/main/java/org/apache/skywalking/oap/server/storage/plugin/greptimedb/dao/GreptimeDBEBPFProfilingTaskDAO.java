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
import org.apache.skywalking.oap.server.core.profiling.ebpf.storage.EBPFProfilingTargetType;
import org.apache.skywalking.oap.server.core.profiling.ebpf.storage.EBPFProfilingTaskRecord;
import org.apache.skywalking.oap.server.core.profiling.ebpf.storage.EBPFProfilingTriggerType;
import org.apache.skywalking.oap.server.core.storage.profiling.ebpf.IEBPFProfilingTaskDAO;
import org.apache.skywalking.oap.server.library.util.CollectionUtils;
import org.apache.skywalking.oap.server.library.util.StringUtil;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBConverter;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBStorageClient;

@RequiredArgsConstructor
public class GreptimeDBEBPFProfilingTaskDAO implements IEBPFProfilingTaskDAO {
    private final GreptimeDBStorageClient client;

    @Override
    public List<EBPFProfilingTaskRecord> queryTasksByServices(
            final List<String> serviceIdList, final EBPFProfilingTriggerType triggerType,
            final long taskStartTime, final long latestUpdateTime) throws IOException {
        if (CollectionUtils.isEmpty(serviceIdList)) {
            return new ArrayList<>();
        }
        final StringBuilder sql = new StringBuilder();
        final List<Object> params = new ArrayList<>();
        final String placeholders = serviceIdList.stream().map(id -> "?")
            .collect(Collectors.joining(","));
        sql.append("select * from ").append(EBPFProfilingTaskRecord.INDEX_NAME)
            .append(" where ").append(EBPFProfilingTaskRecord.SERVICE_ID)
            .append(" in (").append(placeholders).append(")");
        params.addAll(serviceIdList);

        if (triggerType != null) {
            sql.append(" and ").append(EBPFProfilingTaskRecord.TRIGGER_TYPE).append(" = ?");
            params.add(triggerType.value());
        }
        if (taskStartTime > 0) {
            sql.append(" and ").append(EBPFProfilingTaskRecord.START_TIME).append(" >= ?");
            params.add(taskStartTime);
        }
        if (latestUpdateTime > 0) {
            sql.append(" and ").append(EBPFProfilingTaskRecord.LAST_UPDATE_TIME).append(" >= ?");
            params.add(latestUpdateTime);
        }

        return executeQuery(sql.toString(), params);
    }

    @Override
    public List<EBPFProfilingTaskRecord> queryTasksByTargets(
            final String serviceId, final String serviceInstanceId,
            final List<EBPFProfilingTargetType> targetTypes,
            final EBPFProfilingTriggerType triggerType,
            final long taskStartTime, final long latestUpdateTime) throws IOException {
        final StringBuilder sql = new StringBuilder();
        final List<Object> params = new ArrayList<>();
        sql.append("select * from ").append(EBPFProfilingTaskRecord.INDEX_NAME)
            .append(" where ").append(EBPFProfilingTaskRecord.SERVICE_ID).append(" = ?");
        params.add(serviceId);

        if (StringUtil.isNotEmpty(serviceInstanceId)) {
            sql.append(" and ").append(EBPFProfilingTaskRecord.INSTANCE_ID).append(" = ?");
            params.add(serviceInstanceId);
        }
        if (CollectionUtils.isNotEmpty(targetTypes)) {
            final String placeholders = targetTypes.stream().map(t -> "?")
                .collect(Collectors.joining(","));
            sql.append(" and ").append(EBPFProfilingTaskRecord.TARGET_TYPE)
                .append(" in (").append(placeholders).append(")");
            for (final EBPFProfilingTargetType type : targetTypes) {
                params.add(type.value());
            }
        }
        if (triggerType != null) {
            sql.append(" and ").append(EBPFProfilingTaskRecord.TRIGGER_TYPE).append(" = ?");
            params.add(triggerType.value());
        }
        if (taskStartTime > 0) {
            sql.append(" and ").append(EBPFProfilingTaskRecord.START_TIME).append(" >= ?");
            params.add(taskStartTime);
        }
        if (latestUpdateTime > 0) {
            sql.append(" and ").append(EBPFProfilingTaskRecord.LAST_UPDATE_TIME).append(" >= ?");
            params.add(latestUpdateTime);
        }

        return executeQuery(sql.toString(), params);
    }

    @Override
    public List<EBPFProfilingTaskRecord> getTaskRecord(final String id) throws IOException {
        final String sql = "select * from " + EBPFProfilingTaskRecord.INDEX_NAME
            + " where " + EBPFProfilingTaskRecord.LOGICAL_ID + " = ?";
        final List<Object> params = new ArrayList<>();
        params.add(id);
        return executeQuery(sql, params);
    }

    private List<EBPFProfilingTaskRecord> executeQuery(
            final String sql, final List<Object> params) throws IOException {
        final List<EBPFProfilingTaskRecord> records = new ArrayList<>();
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < params.size(); i++) {
                ps.setObject(i + 1, params.get(i));
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    final Map<String, Object> map = GreptimeDBConverter.resultSetToGenericMap(rs);
                    records.add(new EBPFProfilingTaskRecord.Builder()
                        .storage2Entity(new GreptimeDBConverter.ToEntity(map)));
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to query eBPF profiling tasks", e);
        }
        return records;
    }
}
