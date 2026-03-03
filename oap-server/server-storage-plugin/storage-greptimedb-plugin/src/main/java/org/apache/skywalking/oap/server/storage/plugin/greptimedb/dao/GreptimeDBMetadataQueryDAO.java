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

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.skywalking.oap.server.core.analysis.IDManager;
import org.apache.skywalking.oap.server.core.analysis.TimeBucket;
import org.apache.skywalking.oap.server.core.analysis.manual.endpoint.EndpointTraffic;
import org.apache.skywalking.oap.server.core.analysis.manual.instance.InstanceTraffic;
import org.apache.skywalking.oap.server.core.analysis.manual.process.ProcessDetectType;
import org.apache.skywalking.oap.server.core.analysis.manual.process.ProcessTraffic;
import org.apache.skywalking.oap.server.core.analysis.manual.service.ServiceTraffic;
import org.apache.skywalking.oap.server.core.query.enumeration.Language;
import org.apache.skywalking.oap.server.core.query.enumeration.ProfilingSupportStatus;
import org.apache.skywalking.oap.server.core.query.input.Duration;
import org.apache.skywalking.oap.server.core.query.type.Attribute;
import org.apache.skywalking.oap.server.core.query.type.Endpoint;
import org.apache.skywalking.oap.server.core.query.type.Process;
import org.apache.skywalking.oap.server.core.query.type.Service;
import org.apache.skywalking.oap.server.core.query.type.ServiceInstance;
import org.apache.skywalking.oap.server.core.storage.query.IMetadataQueryDAO;
import org.apache.skywalking.oap.server.library.util.StringUtil;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBConverter;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBStorageClient;

import static org.apache.skywalking.oap.server.core.analysis.manual.instance.InstanceTraffic.PropertyUtil.LANGUAGE;
import static org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBQueryHelper.setParameters;

@Slf4j
public class GreptimeDBMetadataQueryDAO implements IMetadataQueryDAO {
    private static final Gson GSON = new Gson();

    private final GreptimeDBStorageClient client;
    private final int metadataQueryMaxSize;

    public GreptimeDBMetadataQueryDAO(final GreptimeDBStorageClient client,
                                       final int metadataQueryMaxSize) {
        this.client = client;
        this.metadataQueryMaxSize = metadataQueryMaxSize;
    }

    @Override
    public List<Service> listServices() throws IOException {
        final String sql = "select * from " + GreptimeDBConverter.resolveTrafficTableName(ServiceTraffic.INDEX_NAME)
            + " limit " + metadataQueryMaxSize;
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            return buildServices(rs);
        } catch (SQLException e) {
            throw new IOException("Failed to list services", e);
        }
    }

    @Override
    public List<ServiceInstance> listInstances(@Nullable final Duration duration,
                                                final String serviceId) throws IOException {
        final StringBuilder sql = new StringBuilder();
        final List<Object> params = new ArrayList<>();
        sql.append("select * from ").append(GreptimeDBConverter.resolveTrafficTableName(InstanceTraffic.INDEX_NAME));
        sql.append(" where ").append(InstanceTraffic.SERVICE_ID).append(" = ?");
        params.add(serviceId);
        if (duration != null) {
            final long startMinuteTimeBucket = TimeBucket.getMinuteTimeBucket(duration.getStartTimestamp());
            final long endMinuteTimeBucket = TimeBucket.getMinuteTimeBucket(duration.getEndTimestamp());
            sql.append(" and ").append(InstanceTraffic.LAST_PING_TIME_BUCKET).append(" >= ?");
            params.add(startMinuteTimeBucket);
            sql.append(" and ").append(InstanceTraffic.TIME_BUCKET).append(" <= ?");
            params.add(endMinuteTimeBucket);
        }
        sql.append(" limit ").append(metadataQueryMaxSize);

        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            setParameters(ps, params);
            try (ResultSet rs = ps.executeQuery()) {
                return buildInstances(rs);
            }
        } catch (SQLException e) {
            throw new IOException("Failed to list instances", e);
        }
    }

    @Override
    public ServiceInstance getInstance(final String instanceId) throws IOException {
        final String sql = "select * from " + GreptimeDBConverter.resolveTrafficTableName(InstanceTraffic.INDEX_NAME)
            + " where `id` = ? limit 1";
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, instanceId);
            try (ResultSet rs = ps.executeQuery()) {
                final List<ServiceInstance> instances = buildInstances(rs);
                return instances.isEmpty() ? null : instances.get(0);
            }
        } catch (SQLException e) {
            throw new IOException("Failed to get instance: " + instanceId, e);
        }
    }

    @Override
    public List<ServiceInstance> getInstances(final List<String> instanceIds) throws IOException {
        if (instanceIds.isEmpty()) {
            return new ArrayList<>();
        }
        final String placeholders = instanceIds.stream().map(id -> "?")
            .collect(Collectors.joining(","));
        final String sql = "select * from " + GreptimeDBConverter.resolveTrafficTableName(InstanceTraffic.INDEX_NAME)
            + " where `id` in (" + placeholders + ") limit " + instanceIds.size();
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < instanceIds.size(); i++) {
                ps.setString(i + 1, instanceIds.get(i));
            }
            try (ResultSet rs = ps.executeQuery()) {
                return buildInstances(rs);
            }
        } catch (SQLException e) {
            throw new IOException("Failed to get instances", e);
        }
    }

    @Override
    public List<Endpoint> findEndpoint(final String keyword, final String serviceId,
                                        final int limit, final Duration duration) throws IOException {
        final StringBuilder sql = new StringBuilder();
        final List<Object> params = new ArrayList<>();
        sql.append("select * from ").append(GreptimeDBConverter.resolveTrafficTableName(EndpointTraffic.INDEX_NAME));
        sql.append(" where ").append(EndpointTraffic.SERVICE_ID).append(" = ?");
        params.add(serviceId);
        if (!Strings.isNullOrEmpty(keyword)) {
            sql.append(" and ").append(EndpointTraffic.NAME).append(" like concat('%',?,'%')");
            params.add(keyword);
        }
        if (duration != null) {
            final long startMinuteTimeBucket = TimeBucket.getMinuteTimeBucket(duration.getStartTimestamp());
            final long endMinuteTimeBucket = TimeBucket.getMinuteTimeBucket(duration.getEndTimestamp());
            sql.append(" and ").append(EndpointTraffic.LAST_PING_TIME_BUCKET).append(" >= ?");
            params.add(startMinuteTimeBucket);
            sql.append(" and ").append(EndpointTraffic.TIME_BUCKET).append(" <= ?");
            params.add(endMinuteTimeBucket);
        }
        sql.append(" order by ").append(EndpointTraffic.TIME_BUCKET).append(" desc");
        sql.append(" limit ").append(limit);

        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            setParameters(ps, params);
            try (ResultSet rs = ps.executeQuery()) {
                final List<Endpoint> endpoints = new ArrayList<>();
                while (rs.next()) {
                    final Map<String, Object> map = GreptimeDBConverter.resultSetToGenericMap(rs);
                    final EndpointTraffic traffic = new EndpointTraffic.Builder()
                        .storage2Entity(new GreptimeDBConverter.ToEntity(map));
                    final Endpoint endpoint = new Endpoint();
                    endpoint.setId(traffic.id().build());
                    endpoint.setName(traffic.getName());
                    endpoints.add(endpoint);
                }
                return endpoints;
            }
        } catch (SQLException e) {
            throw new IOException("Failed to find endpoints", e);
        }
    }

    @Override
    public List<Process> listProcesses(final String serviceId,
                                        final ProfilingSupportStatus supportStatus,
                                        final long lastPingStartTimeBucket,
                                        final long lastPingEndTimeBucket) throws IOException {
        final StringBuilder sql = new StringBuilder();
        final List<Object> params = new ArrayList<>();
        sql.append("select * from ").append(GreptimeDBConverter.resolveTrafficTableName(ProcessTraffic.INDEX_NAME));
        sql.append(" where 1=1");
        appendProcessConditions(sql, params, serviceId, null, null,
            supportStatus, lastPingStartTimeBucket, lastPingEndTimeBucket, false);
        sql.append(" limit ").append(metadataQueryMaxSize);
        return executeProcessQuery(sql.toString(), params);
    }

    @Override
    public List<Process> listProcesses(final String serviceInstanceId,
                                        final Duration duration,
                                        final boolean includeVirtual) throws IOException {
        final StringBuilder sql = new StringBuilder();
        final List<Object> params = new ArrayList<>();
        sql.append("select * from ").append(GreptimeDBConverter.resolveTrafficTableName(ProcessTraffic.INDEX_NAME));
        sql.append(" where 1=1");
        appendProcessConditions(sql, params, null, serviceInstanceId, null,
            null, duration.getStartTimeBucket(), duration.getEndTimeBucket(), includeVirtual);
        sql.append(" limit ").append(metadataQueryMaxSize);
        return executeProcessQuery(sql.toString(), params);
    }

    @Override
    public List<Process> listProcesses(final String agentId,
                                        final long startPingTimeBucket,
                                        final long endPingTimeBucket) throws IOException {
        final StringBuilder sql = new StringBuilder();
        final List<Object> params = new ArrayList<>();
        sql.append("select * from ").append(GreptimeDBConverter.resolveTrafficTableName(ProcessTraffic.INDEX_NAME));
        sql.append(" where 1=1");
        appendProcessConditions(sql, params, null, null, agentId,
            null, 0, 0, false);
        sql.append(" limit ").append(metadataQueryMaxSize);
        return executeProcessQuery(sql.toString(), params);
    }

    @Override
    public long getProcessCount(final String serviceId,
                                 final ProfilingSupportStatus profilingSupportStatus,
                                 final long lastPingStartTimeBucket,
                                 final long lastPingEndTimeBucket) throws IOException {
        final StringBuilder sql = new StringBuilder();
        final List<Object> params = new ArrayList<>();
        sql.append("select count(1) as total from ").append(GreptimeDBConverter.resolveTrafficTableName(ProcessTraffic.INDEX_NAME));
        sql.append(" where 1=1");
        appendProcessConditions(sql, params, serviceId, null, null,
            profilingSupportStatus, lastPingStartTimeBucket, lastPingEndTimeBucket, false);
        return executeCountQuery(sql.toString(), params);
    }

    @Override
    public long getProcessCount(final String instanceId) throws IOException {
        final StringBuilder sql = new StringBuilder();
        final List<Object> params = new ArrayList<>();
        sql.append("select count(1) as total from ").append(GreptimeDBConverter.resolveTrafficTableName(ProcessTraffic.INDEX_NAME));
        sql.append(" where 1=1");
        appendProcessConditions(sql, params, null, instanceId, null,
            null, 0, 0, false);
        return executeCountQuery(sql.toString(), params);
    }

    @Override
    public Process getProcess(final String processId) throws IOException {
        final String sql = "select * from " + GreptimeDBConverter.resolveTrafficTableName(ProcessTraffic.INDEX_NAME)
            + " where `id` = ? limit 1";
        final List<Object> params = new ArrayList<>();
        params.add(processId);
        final List<Process> processes = executeProcessQuery(sql, params);
        return processes.isEmpty() ? null : processes.get(0);
    }

    private List<Service> buildServices(final ResultSet rs) throws SQLException {
        final List<Service> services = new ArrayList<>();
        while (rs.next()) {
            final Map<String, Object> map = GreptimeDBConverter.resultSetToGenericMap(rs);
            final ServiceTraffic traffic = new ServiceTraffic.Builder()
                .storage2Entity(new GreptimeDBConverter.ToEntity(map));
            final Service service = new Service();
            service.setId(traffic.getServiceId());
            service.setName(traffic.getName());
            service.setShortName(traffic.getShortName());
            service.setGroup(traffic.getGroup());
            service.getLayers().add(traffic.getLayer().name());
            services.add(service);
        }
        return services;
    }

    private List<ServiceInstance> buildInstances(final ResultSet rs) throws SQLException {
        final List<ServiceInstance> instances = new ArrayList<>();
        while (rs.next()) {
            final Map<String, Object> map = GreptimeDBConverter.resultSetToGenericMap(rs);
            final InstanceTraffic traffic = new InstanceTraffic.Builder()
                .storage2Entity(new GreptimeDBConverter.ToEntity(map));
            final ServiceInstance instance = new ServiceInstance();
            instance.setId(traffic.id().build());
            instance.setName(traffic.getName());
            instance.setInstanceUUID(instance.getId());

            final JsonObject properties = traffic.getProperties();
            if (properties != null) {
                for (final Map.Entry<String, JsonElement> property : properties.entrySet()) {
                    final String key = property.getKey();
                    final String value = property.getValue().getAsString();
                    if (key.equals(LANGUAGE)) {
                        instance.setLanguage(Language.value(value));
                    } else {
                        instance.getAttributes().add(new Attribute(key, value));
                    }
                }
            } else {
                instance.setLanguage(Language.UNKNOWN);
            }
            instances.add(instance);
        }
        return instances;
    }

    private List<Process> buildProcesses(final ResultSet rs) throws SQLException {
        final List<Process> processes = new ArrayList<>();
        while (rs.next()) {
            final Map<String, Object> map = GreptimeDBConverter.resultSetToGenericMap(rs);
            final ProcessTraffic traffic = new ProcessTraffic.Builder()
                .storage2Entity(new GreptimeDBConverter.ToEntity(map));
            final Process process = new Process();
            process.setId(traffic.id().build());
            process.setName(traffic.getName());
            final String serviceId = traffic.getServiceId();
            process.setServiceId(serviceId);
            process.setServiceName(IDManager.ServiceID.analysisId(serviceId).getName());
            final String instanceId = traffic.getInstanceId();
            process.setInstanceId(instanceId);
            process.setInstanceName(IDManager.ServiceInstanceID.analysisId(instanceId).getName());
            process.setAgentId(traffic.getAgentId());
            process.setDetectType(ProcessDetectType.valueOf(traffic.getDetectType()).name());
            process.setProfilingSupportStatus(
                ProfilingSupportStatus.valueOf(traffic.getProfilingSupportStatus()).name());

            final JsonObject properties = traffic.getProperties();
            if (properties != null) {
                for (final Map.Entry<String, JsonElement> property : properties.entrySet()) {
                    process.getAttributes().add(
                        new Attribute(property.getKey(), property.getValue().getAsString()));
                }
            }
            final String labelsJson = traffic.getLabelsJson();
            if (StringUtils.isNotEmpty(labelsJson)) {
                @SuppressWarnings("unchecked")
                final List<String> labels = GSON.fromJson(labelsJson, ArrayList.class);
                process.getLabels().addAll(labels);
            }
            processes.add(process);
        }
        return processes;
    }

    private void appendProcessConditions(final StringBuilder sql, final List<Object> params,
                                          final String serviceId, final String instanceId,
                                          final String agentId,
                                          final ProfilingSupportStatus profilingSupportStatus,
                                          final long lastPingStartTimeBucket,
                                          final long lastPingEndTimeBucket,
                                          final boolean includeVirtual) {
        if (StringUtil.isNotEmpty(serviceId)) {
            sql.append(" and ").append(ProcessTraffic.SERVICE_ID).append(" = ?");
            params.add(serviceId);
        }
        if (StringUtil.isNotEmpty(instanceId)) {
            sql.append(" and ").append(ProcessTraffic.INSTANCE_ID).append(" = ?");
            params.add(instanceId);
        }
        if (StringUtil.isNotEmpty(agentId)) {
            sql.append(" and ").append(ProcessTraffic.AGENT_ID).append(" = ?");
            params.add(agentId);
        }
        if (profilingSupportStatus != null) {
            sql.append(" and ").append(ProcessTraffic.PROFILING_SUPPORT_STATUS).append(" = ?");
            params.add(profilingSupportStatus.value());
        }
        if (lastPingStartTimeBucket > 0) {
            sql.append(" and ").append(ProcessTraffic.LAST_PING_TIME_BUCKET).append(" >= ?");
            params.add(lastPingStartTimeBucket);
        }
        if (lastPingEndTimeBucket > 0) {
            sql.append(" and ").append(ProcessTraffic.TIME_BUCKET).append(" <= ?");
            params.add(lastPingEndTimeBucket);
        }
        if (!includeVirtual) {
            sql.append(" and ").append(ProcessTraffic.DETECT_TYPE).append(" != ?");
            params.add(ProcessDetectType.VIRTUAL.value());
        }
    }

    private List<Process> executeProcessQuery(final String sql,
                                               final List<Object> params) throws IOException {
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            setParameters(ps, params);
            try (ResultSet rs = ps.executeQuery()) {
                return buildProcesses(rs);
            }
        } catch (SQLException e) {
            throw new IOException("Failed to query processes", e);
        }
    }

    private long executeCountQuery(final String sql, final List<Object> params) throws IOException {
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            setParameters(ps, params);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong("total");
                }
                return 0L;
            }
        } catch (SQLException e) {
            throw new IOException("Failed to count processes", e);
        }
    }

}
