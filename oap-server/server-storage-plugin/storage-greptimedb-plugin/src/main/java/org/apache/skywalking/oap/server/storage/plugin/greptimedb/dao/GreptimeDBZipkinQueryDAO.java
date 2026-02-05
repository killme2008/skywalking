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

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import org.apache.skywalking.oap.server.core.query.input.Duration;
import org.apache.skywalking.oap.server.core.storage.query.IZipkinQueryDAO;
import org.apache.skywalking.oap.server.core.zipkin.ZipkinServiceRelationTraffic;
import org.apache.skywalking.oap.server.core.zipkin.ZipkinServiceSpanTraffic;
import org.apache.skywalking.oap.server.core.zipkin.ZipkinServiceTraffic;
import org.apache.skywalking.oap.server.core.zipkin.ZipkinSpanRecord;
import org.apache.skywalking.oap.server.library.util.CollectionUtils;
import org.apache.skywalking.oap.server.library.util.StringUtil;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBConverter;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBStorageClient;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.storage.QueryRequest;

@RequiredArgsConstructor
public class GreptimeDBZipkinQueryDAO implements IZipkinQueryDAO {
    private static final int NAME_QUERY_MAX_SIZE = Integer.MAX_VALUE;
    private static final Gson GSON = new Gson();
    private final GreptimeDBStorageClient client;

    @Override
    public List<String> getServiceNames() throws IOException {
        final String sql = "select " + ZipkinServiceTraffic.SERVICE_NAME
            + " from " + GreptimeDBConverter.resolveTrafficTableName(ZipkinServiceTraffic.INDEX_NAME)
            + " limit " + NAME_QUERY_MAX_SIZE;
        final List<String> services = new ArrayList<>();
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                services.add(rs.getString(ZipkinServiceTraffic.SERVICE_NAME));
            }
        } catch (SQLException e) {
            throw new IOException("Failed to query Zipkin service names", e);
        }
        return services;
    }

    @Override
    public List<String> getRemoteServiceNames(final String serviceName) throws IOException {
        final String sql = "select " + ZipkinServiceRelationTraffic.REMOTE_SERVICE_NAME
            + " from " + GreptimeDBConverter.resolveTrafficTableName(ZipkinServiceRelationTraffic.INDEX_NAME)
            + " where " + ZipkinServiceRelationTraffic.SERVICE_NAME + " = ?"
            + " limit " + NAME_QUERY_MAX_SIZE;
        final List<String> remoteServices = new ArrayList<>();
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, serviceName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    remoteServices.add(rs.getString(
                        ZipkinServiceRelationTraffic.REMOTE_SERVICE_NAME));
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to query Zipkin remote service names", e);
        }
        return remoteServices;
    }

    @Override
    public List<String> getSpanNames(final String serviceName) throws IOException {
        final String sql = "select " + ZipkinServiceSpanTraffic.SPAN_NAME
            + " from " + GreptimeDBConverter.resolveTrafficTableName(ZipkinServiceSpanTraffic.INDEX_NAME)
            + " where " + ZipkinServiceSpanTraffic.SERVICE_NAME + " = ?"
            + " limit " + NAME_QUERY_MAX_SIZE;
        final List<String> spanNames = new ArrayList<>();
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, serviceName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    spanNames.add(rs.getString(ZipkinServiceSpanTraffic.SPAN_NAME));
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to query Zipkin span names", e);
        }
        return spanNames;
    }

    @Override
    public List<Span> getTrace(final String traceId,
                               @Nullable final Duration duration) throws IOException {
        final String sql = "select * from " + ZipkinSpanRecord.INDEX_NAME
            + " where " + ZipkinSpanRecord.TRACE_ID + " = ?";
        final List<Span> trace = new ArrayList<>();
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, traceId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    trace.add(buildSpan(rs));
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to query Zipkin trace: " + traceId, e);
        }
        return trace;
    }

    @Override
    public List<List<Span>> getTraces(final QueryRequest request,
                                      final Duration duration) throws IOException {
        final long startTimeMillis = duration.getStartTimestamp();
        final long endTimeMillis = duration.getEndTimestamp();

        final StringBuilder sql = new StringBuilder();
        final List<Object> params = new ArrayList<>();
        sql.append("select ").append(ZipkinSpanRecord.TRACE_ID)
            .append(", min(").append(ZipkinSpanRecord.TIMESTAMP_MILLIS).append(")")
            .append(" from ").append(ZipkinSpanRecord.INDEX_NAME)
            .append(" where 1=1");

        if (startTimeMillis > 0 && endTimeMillis > 0) {
            sql.append(" and ").append(ZipkinSpanRecord.TIMESTAMP_MILLIS).append(" >= ?");
            params.add(startTimeMillis);
            sql.append(" and ").append(ZipkinSpanRecord.TIMESTAMP_MILLIS).append(" <= ?");
            params.add(endTimeMillis);
        }
        if (request.minDuration() != null) {
            sql.append(" and ").append(ZipkinSpanRecord.DURATION).append(" >= ?");
            params.add(request.minDuration());
        }
        if (request.maxDuration() != null) {
            sql.append(" and ").append(ZipkinSpanRecord.DURATION).append(" <= ?");
            params.add(request.maxDuration());
        }
        if (!StringUtil.isEmpty(request.serviceName())) {
            sql.append(" and ").append(ZipkinSpanRecord.LOCAL_ENDPOINT_SERVICE_NAME)
                .append(" = ?");
            params.add(request.serviceName());
        }
        if (!StringUtil.isEmpty(request.remoteServiceName())) {
            sql.append(" and ").append(ZipkinSpanRecord.REMOTE_ENDPOINT_SERVICE_NAME)
                .append(" = ?");
            params.add(request.remoteServiceName());
        }
        if (!StringUtil.isEmpty(request.spanName())) {
            sql.append(" and ").append(ZipkinSpanRecord.NAME).append(" = ?");
            params.add(request.spanName());
        }
        if (CollectionUtils.isNotEmpty(request.annotationQuery())) {
            for (final Map.Entry<String, String> annotation
                    : request.annotationQuery().entrySet()) {
                sql.append(" and ").append(ZipkinSpanRecord.QUERY).append(" LIKE ?");
                if (annotation.getValue().isEmpty()) {
                    params.add("%" + annotation.getKey() + "%");
                } else {
                    params.add("%" + annotation.getKey() + "="
                        + annotation.getValue() + "%");
                }
            }
        }

        sql.append(" group by ").append(ZipkinSpanRecord.TRACE_ID);
        sql.append(" order by min(").append(ZipkinSpanRecord.TIMESTAMP_MILLIS)
            .append(") desc");
        sql.append(" limit ").append(request.limit());

        final Set<String> traceIds = new HashSet<>();
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) {
                ps.setObject(i + 1, params.get(i));
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    traceIds.add(rs.getString(ZipkinSpanRecord.TRACE_ID));
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to query Zipkin traces", e);
        }

        return getTraces(traceIds, duration);
    }

    @Override
    public List<List<Span>> getTraces(final Set<String> traceIds,
                                      @Nullable final Duration duration) throws IOException {
        if (CollectionUtils.isEmpty(traceIds)) {
            return new ArrayList<>();
        }

        final String placeholders = traceIds.stream().map(id -> "?")
            .collect(Collectors.joining(","));
        final String sql = "select * from " + ZipkinSpanRecord.INDEX_NAME
            + " where " + ZipkinSpanRecord.TRACE_ID
            + " in (" + placeholders + ")"
            + " order by " + ZipkinSpanRecord.TIMESTAMP_MILLIS + " desc";

        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            int idx = 1;
            for (final String traceId : traceIds) {
                ps.setString(idx++, traceId);
            }
            try (ResultSet rs = ps.executeQuery()) {
                return buildTraces(rs);
            }
        } catch (SQLException e) {
            throw new IOException("Failed to query Zipkin traces by IDs", e);
        }
    }

    private List<List<Span>> buildTraces(final ResultSet rs) throws SQLException {
        final Map<String, List<Span>> groupedByTraceId = new LinkedHashMap<>();
        while (rs.next()) {
            final Span span = buildSpan(rs);
            final String traceId = span.traceId();
            groupedByTraceId.computeIfAbsent(traceId, k -> new ArrayList<>())
                .add(span);
        }
        return new ArrayList<>(groupedByTraceId.values());
    }

    private Span buildSpan(final ResultSet rs) throws SQLException {
        final Span.Builder span = Span.newBuilder();
        span.traceId(rs.getString(ZipkinSpanRecord.TRACE_ID));
        span.id(rs.getString(ZipkinSpanRecord.SPAN_ID));
        span.parentId(rs.getString(ZipkinSpanRecord.PARENT_ID));
        final String kind = rs.getString(ZipkinSpanRecord.KIND);
        if (!StringUtil.isEmpty(kind)) {
            span.kind(Span.Kind.valueOf(kind));
        }
        span.timestamp(rs.getLong(ZipkinSpanRecord.TIMESTAMP));
        span.duration(rs.getLong(ZipkinSpanRecord.DURATION));
        span.name(rs.getString(ZipkinSpanRecord.NAME));
        span.debug(rs.getInt(ZipkinSpanRecord.DEBUG) != 0);
        span.shared(rs.getInt(ZipkinSpanRecord.SHARED) != 0);

        final Endpoint.Builder localEndpoint = Endpoint.newBuilder();
        localEndpoint.serviceName(
            rs.getString(ZipkinSpanRecord.LOCAL_ENDPOINT_SERVICE_NAME));
        if (!StringUtil.isEmpty(
                rs.getString(ZipkinSpanRecord.LOCAL_ENDPOINT_IPV4))) {
            localEndpoint.parseIp(
                rs.getString(ZipkinSpanRecord.LOCAL_ENDPOINT_IPV4));
        } else {
            localEndpoint.parseIp(
                rs.getString(ZipkinSpanRecord.LOCAL_ENDPOINT_IPV6));
        }
        localEndpoint.port(rs.getInt(ZipkinSpanRecord.LOCAL_ENDPOINT_PORT));
        span.localEndpoint(localEndpoint.build());

        final Endpoint.Builder remoteEndpoint = Endpoint.newBuilder();
        remoteEndpoint.serviceName(
            rs.getString(ZipkinSpanRecord.REMOTE_ENDPOINT_SERVICE_NAME));
        if (!StringUtil.isEmpty(
                rs.getString(ZipkinSpanRecord.REMOTE_ENDPOINT_IPV4))) {
            remoteEndpoint.parseIp(
                rs.getString(ZipkinSpanRecord.REMOTE_ENDPOINT_IPV4));
        } else {
            remoteEndpoint.parseIp(
                rs.getString(ZipkinSpanRecord.REMOTE_ENDPOINT_IPV6));
        }
        remoteEndpoint.port(rs.getInt(ZipkinSpanRecord.REMOTE_ENDPOINT_PORT));
        span.remoteEndpoint(remoteEndpoint.build());

        final String tagsString = rs.getString(ZipkinSpanRecord.TAGS);
        if (!StringUtil.isEmpty(tagsString)) {
            final JsonObject tagsJson = GSON.fromJson(tagsString, JsonObject.class);
            for (final Map.Entry<String, JsonElement> tag : tagsJson.entrySet()) {
                span.putTag(tag.getKey(), tag.getValue().getAsString());
            }
        }

        final String annotationString = rs.getString(ZipkinSpanRecord.ANNOTATIONS);
        if (!StringUtil.isEmpty(annotationString)) {
            final JsonObject annotationJson = GSON.fromJson(
                annotationString, JsonObject.class);
            for (final Map.Entry<String, JsonElement> annotation
                    : annotationJson.entrySet()) {
                span.addAnnotation(Long.parseLong(annotation.getKey()),
                    annotation.getValue().getAsString());
            }
        }

        return span.build();
    }
}
