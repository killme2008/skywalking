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
import lombok.RequiredArgsConstructor;
import org.apache.skywalking.oap.server.core.analysis.manual.relation.endpoint.EndpointRelationServerSideMetrics;
import org.apache.skywalking.oap.server.core.analysis.manual.relation.instance.ServiceInstanceRelationClientSideMetrics;
import org.apache.skywalking.oap.server.core.analysis.manual.relation.instance.ServiceInstanceRelationServerSideMetrics;
import org.apache.skywalking.oap.server.core.analysis.manual.relation.process.ProcessRelationClientSideMetrics;
import org.apache.skywalking.oap.server.core.analysis.manual.relation.process.ProcessRelationServerSideMetrics;
import org.apache.skywalking.oap.server.core.analysis.manual.relation.service.ServiceRelationClientSideMetrics;
import org.apache.skywalking.oap.server.core.analysis.manual.relation.service.ServiceRelationServerSideMetrics;
import org.apache.skywalking.oap.server.core.analysis.metrics.IntList;
import org.apache.skywalking.oap.server.core.analysis.metrics.Metrics;
import org.apache.skywalking.oap.server.core.query.input.Duration;
import org.apache.skywalking.oap.server.core.query.type.Call;
import org.apache.skywalking.oap.server.core.source.DetectPoint;
import org.apache.skywalking.oap.server.core.storage.query.ITopologyQueryDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBConverter;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBStorageClient;

import static org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBQueryHelper.appendTimestampCondition;
import static org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBQueryHelper.setParameters;

@RequiredArgsConstructor
public class GreptimeDBTopologyQueryDAO implements ITopologyQueryDAO {
    private final GreptimeDBStorageClient client;

    @Override
    public List<Call.CallDetail> loadServiceRelationsDetectedAtServerSide(
            final Duration duration, final List<String> serviceIds) throws IOException {
        return loadServiceCalls(
            ServiceRelationServerSideMetrics.INDEX_NAME, duration,
            ServiceRelationServerSideMetrics.SOURCE_SERVICE_ID,
            ServiceRelationServerSideMetrics.DEST_SERVICE_ID, serviceIds, DetectPoint.SERVER
        );
    }

    @Override
    public List<Call.CallDetail> loadServiceRelationDetectedAtClientSide(
            final Duration duration, final List<String> serviceIds) throws IOException {
        return loadServiceCalls(
            ServiceRelationClientSideMetrics.INDEX_NAME, duration,
            ServiceRelationClientSideMetrics.SOURCE_SERVICE_ID,
            ServiceRelationClientSideMetrics.DEST_SERVICE_ID, serviceIds, DetectPoint.CLIENT
        );
    }

    @Override
    public List<Call.CallDetail> loadServiceRelationsDetectedAtServerSide(
            final Duration duration) throws IOException {
        return loadServiceCalls(
            ServiceRelationServerSideMetrics.INDEX_NAME, duration,
            ServiceRelationServerSideMetrics.SOURCE_SERVICE_ID,
            ServiceRelationServerSideMetrics.DEST_SERVICE_ID, new ArrayList<>(0), DetectPoint.SERVER
        );
    }

    @Override
    public List<Call.CallDetail> loadServiceRelationDetectedAtClientSide(
            final Duration duration) throws IOException {
        return loadServiceCalls(
            ServiceRelationClientSideMetrics.INDEX_NAME, duration,
            ServiceRelationClientSideMetrics.SOURCE_SERVICE_ID,
            ServiceRelationClientSideMetrics.DEST_SERVICE_ID, new ArrayList<>(0), DetectPoint.CLIENT
        );
    }

    @Override
    public List<Call.CallDetail> loadInstanceRelationDetectedAtServerSide(
            final String clientServiceId, final String serverServiceId,
            final Duration duration) throws IOException {
        return loadServiceInstanceCalls(
            ServiceInstanceRelationServerSideMetrics.INDEX_NAME, duration,
            ServiceInstanceRelationServerSideMetrics.SOURCE_SERVICE_ID,
            ServiceInstanceRelationServerSideMetrics.DEST_SERVICE_ID,
            clientServiceId, serverServiceId, DetectPoint.SERVER
        );
    }

    @Override
    public List<Call.CallDetail> loadInstanceRelationDetectedAtClientSide(
            final String clientServiceId, final String serverServiceId,
            final Duration duration) throws IOException {
        return loadServiceInstanceCalls(
            ServiceInstanceRelationClientSideMetrics.INDEX_NAME, duration,
            ServiceInstanceRelationClientSideMetrics.SOURCE_SERVICE_ID,
            ServiceInstanceRelationClientSideMetrics.DEST_SERVICE_ID,
            clientServiceId, serverServiceId, DetectPoint.CLIENT
        );
    }

    @Override
    public List<Call.CallDetail> loadEndpointRelation(
            final Duration duration, final String destEndpointId) throws IOException {
        final List<Call.CallDetail> calls = loadEndpointFromSide(
            EndpointRelationServerSideMetrics.INDEX_NAME, duration,
            EndpointRelationServerSideMetrics.SOURCE_ENDPOINT,
            EndpointRelationServerSideMetrics.DEST_ENDPOINT, destEndpointId, false
        );
        calls.addAll(loadEndpointFromSide(
            EndpointRelationServerSideMetrics.INDEX_NAME, duration,
            EndpointRelationServerSideMetrics.SOURCE_ENDPOINT,
            EndpointRelationServerSideMetrics.DEST_ENDPOINT, destEndpointId, true
        ));
        return calls;
    }

    @Override
    public List<Call.CallDetail> loadProcessRelationDetectedAtClientSide(
            final String serviceInstanceId, final Duration duration) throws IOException {
        return loadProcessFromSide(duration, serviceInstanceId, DetectPoint.CLIENT);
    }

    @Override
    public List<Call.CallDetail> loadProcessRelationDetectedAtServerSide(
            final String serviceInstanceId, final Duration duration) throws IOException {
        return loadProcessFromSide(duration, serviceInstanceId, DetectPoint.SERVER);
    }

    private String resolveTableName(final String indexName, final Duration duration) {
        return GreptimeDBConverter.resolveMetricsTableName(indexName, duration.getStep());
    }

    private List<Call.CallDetail> loadServiceCalls(final String tableName,
                                                    final Duration duration,
                                                    final String sourceCName,
                                                    final String destCName,
                                                    final List<String> serviceIds,
                                                    final DetectPoint detectPoint) throws IOException {
        final String table = resolveTableName(tableName, duration);
        final StringBuilder sql = new StringBuilder();
        final List<Object> params = new ArrayList<>();

        sql.append("select ").append(Metrics.ENTITY_ID).append(", ")
           .append(ServiceRelationServerSideMetrics.COMPONENT_IDS)
           .append(" from ").append(table)
           .append(" where 1=1");
        appendTimestampCondition(sql, params, duration.getStartTimeBucket(), duration.getEndTimeBucket());

        if (!serviceIds.isEmpty()) {
            sql.append(" and (");
            for (int i = 0; i < serviceIds.size(); i++) {
                if (i > 0) {
                    sql.append(" or ");
                }
                sql.append(sourceCName).append(" = ? or ").append(destCName).append(" = ?");
                params.add(serviceIds.get(i));
                params.add(serviceIds.get(i));
            }
            sql.append(")");
        }
        sql.append(" group by ").append(Metrics.ENTITY_ID)
           .append(", ").append(ServiceRelationServerSideMetrics.COMPONENT_IDS);

        final List<Call.CallDetail> calls = new ArrayList<>();
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            setParameters(ps, params);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    final String entityId = rs.getString(Metrics.ENTITY_ID);
                    final IntList componentIds = new IntList(
                        rs.getString(ServiceRelationServerSideMetrics.COMPONENT_IDS));
                    for (int i = 0; i < componentIds.size(); i++) {
                        final Call.CallDetail call = new Call.CallDetail();
                        call.buildFromServiceRelation(entityId, componentIds.get(i), detectPoint);
                        calls.add(call);
                    }
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to load service calls from " + table, e);
        }
        return calls;
    }

    private List<Call.CallDetail> loadServiceInstanceCalls(final String tableName,
                                                            final Duration duration,
                                                            final String sourceCName,
                                                            final String destCName,
                                                            final String sourceServiceId,
                                                            final String destServiceId,
                                                            final DetectPoint detectPoint) throws IOException {
        final String table = resolveTableName(tableName, duration);
        final StringBuilder sql = new StringBuilder();
        final List<Object> params = new ArrayList<>();

        sql.append("select ").append(Metrics.ENTITY_ID)
           .append(" from ").append(table)
           .append(" where 1=1");
        appendTimestampCondition(sql, params, duration.getStartTimeBucket(), duration.getEndTimeBucket());

        sql.append(" and ((").append(sourceCName).append(" = ? and ").append(destCName).append(" = ?)")
           .append(" or (").append(sourceCName).append(" = ? and ").append(destCName).append(" = ?))");
        params.add(sourceServiceId);
        params.add(destServiceId);
        params.add(destServiceId);
        params.add(sourceServiceId);

        sql.append(" group by ").append(Metrics.ENTITY_ID);

        final List<Call.CallDetail> calls = new ArrayList<>();
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            setParameters(ps, params);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    final Call.CallDetail call = new Call.CallDetail();
                    call.buildFromInstanceRelation(rs.getString(Metrics.ENTITY_ID), detectPoint);
                    calls.add(call);
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to load instance calls from " + table, e);
        }
        return calls;
    }

    private List<Call.CallDetail> loadEndpointFromSide(final String tableName,
                                                        final Duration duration,
                                                        final String sourceCName,
                                                        final String destCName,
                                                        final String id,
                                                        final boolean isSourceId) throws IOException {
        final String table = resolveTableName(tableName, duration);
        final StringBuilder sql = new StringBuilder();
        final List<Object> params = new ArrayList<>();

        sql.append("select ").append(Metrics.ENTITY_ID)
           .append(" from ").append(table)
           .append(" where 1=1");
        appendTimestampCondition(sql, params, duration.getStartTimeBucket(), duration.getEndTimeBucket());
        sql.append(" and ").append(isSourceId ? sourceCName : destCName).append(" = ?");
        params.add(id);

        sql.append(" group by ").append(Metrics.ENTITY_ID);

        final List<Call.CallDetail> calls = new ArrayList<>();
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            setParameters(ps, params);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    final Call.CallDetail call = new Call.CallDetail();
                    call.buildFromEndpointRelation(rs.getString(Metrics.ENTITY_ID), DetectPoint.SERVER);
                    calls.add(call);
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to load endpoint calls from " + table, e);
        }
        return calls;
    }

    private List<Call.CallDetail> loadProcessFromSide(final Duration duration,
                                                       final String instanceId,
                                                       final DetectPoint detectPoint) throws IOException {
        final String indexName = detectPoint == DetectPoint.SERVER
            ? ProcessRelationServerSideMetrics.INDEX_NAME
            : ProcessRelationClientSideMetrics.INDEX_NAME;
        final String table = resolveTableName(indexName, duration);

        final StringBuilder sql = new StringBuilder();
        final List<Object> params = new ArrayList<>();

        sql.append("select ").append(Metrics.ENTITY_ID).append(", ")
           .append(ProcessRelationServerSideMetrics.COMPONENT_ID)
           .append(" from ").append(table)
           .append(" where 1=1");
        appendTimestampCondition(sql, params, duration.getStartTimeBucket(), duration.getEndTimeBucket());
        sql.append(" and ").append(ProcessRelationServerSideMetrics.SERVICE_INSTANCE_ID).append(" = ?");
        params.add(instanceId);

        sql.append(" group by ").append(Metrics.ENTITY_ID)
           .append(", ").append(ProcessRelationServerSideMetrics.COMPONENT_ID);

        final List<Call.CallDetail> calls = new ArrayList<>();
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            setParameters(ps, params);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    final Call.CallDetail call = new Call.CallDetail();
                    call.buildProcessRelation(
                        rs.getString(Metrics.ENTITY_ID),
                        rs.getInt(ProcessRelationServerSideMetrics.COMPONENT_ID),
                        detectPoint
                    );
                    calls.add(call);
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to load process calls from " + table, e);
        }
        return calls;
    }

}
