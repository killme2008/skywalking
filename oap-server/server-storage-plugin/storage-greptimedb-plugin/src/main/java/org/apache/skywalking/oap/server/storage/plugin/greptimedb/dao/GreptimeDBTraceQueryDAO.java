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
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.oap.server.core.analysis.IDManager;
import org.apache.skywalking.oap.server.core.analysis.manual.searchtag.Tag;
import org.apache.skywalking.oap.server.core.analysis.manual.segment.SegmentRecord;
import org.apache.skywalking.oap.server.core.query.input.Duration;
import org.apache.skywalking.oap.server.core.query.type.BasicTrace;
import org.apache.skywalking.oap.server.core.query.type.QueryOrder;
import org.apache.skywalking.oap.server.core.query.type.Span;
import org.apache.skywalking.oap.server.core.query.type.TraceBrief;
import org.apache.skywalking.oap.server.core.query.type.TraceState;
import org.apache.skywalking.oap.server.core.storage.query.ITraceQueryDAO;
import org.apache.skywalking.oap.server.library.util.BooleanUtils;
import org.apache.skywalking.oap.server.library.util.CollectionUtils;

import static org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBQueryHelper.setParameters;
import org.apache.skywalking.oap.server.library.util.StringUtil;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBStorageClient;

import static org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBQueryHelper.buildJsonPathMatchExpr;

@Slf4j
@RequiredArgsConstructor
public class GreptimeDBTraceQueryDAO implements ITraceQueryDAO {
    private static final String DETAIL_SELECT = "select " +
        SegmentRecord.SEGMENT_ID + ", " +
        SegmentRecord.TRACE_ID + ", " +
        SegmentRecord.ENDPOINT_ID + ", " +
        SegmentRecord.SERVICE_ID + ", " +
        SegmentRecord.SERVICE_INSTANCE_ID + ", " +
        SegmentRecord.START_TIME + ", " +
        SegmentRecord.LATENCY + ", " +
        SegmentRecord.IS_ERROR + ", " +
        SegmentRecord.DATA_BINARY;

    private final GreptimeDBStorageClient client;

    @Override
    public TraceBrief queryBasicTraces(final Duration duration,
                                        final long minDuration,
                                        final long maxDuration,
                                        final String serviceId,
                                        final String serviceInstanceId,
                                        final String endpointId,
                                        final String traceId,
                                        final int limit,
                                        final int from,
                                        final TraceState traceState,
                                        final QueryOrder queryOrder,
                                        final List<Tag> tags) throws IOException {
        final StringBuilder sql = new StringBuilder();
        final List<Object> params = new ArrayList<>();

        sql.append("select ");
        sql.append(SegmentRecord.SEGMENT_ID).append(", ");
        sql.append(SegmentRecord.START_TIME).append(", ");
        sql.append(SegmentRecord.ENDPOINT_ID).append(", ");
        sql.append(SegmentRecord.LATENCY).append(", ");
        sql.append(SegmentRecord.IS_ERROR).append(", ");
        sql.append(SegmentRecord.TRACE_ID);
        sql.append(" from ").append(SegmentRecord.INDEX_NAME);
        sql.append(" where 1=1");

        if (duration != null) {
            final long startSecondTB = duration.getStartTimeBucketInSec();
            final long endSecondTB = duration.getEndTimeBucketInSec();
            if (startSecondTB != 0 && endSecondTB != 0) {
                sql.append(" and ").append(SegmentRecord.TIME_BUCKET).append(" >= ?");
                params.add(startSecondTB);
                sql.append(" and ").append(SegmentRecord.TIME_BUCKET).append(" <= ?");
                params.add(endSecondTB);
            }
        }
        if (minDuration != 0) {
            sql.append(" and ").append(SegmentRecord.LATENCY).append(" >= ?");
            params.add(minDuration);
        }
        if (maxDuration != 0) {
            sql.append(" and ").append(SegmentRecord.LATENCY).append(" <= ?");
            params.add(maxDuration);
        }
        if (StringUtil.isNotEmpty(serviceId)) {
            sql.append(" and ").append(SegmentRecord.SERVICE_ID).append(" = ?");
            params.add(serviceId);
        }
        if (StringUtil.isNotEmpty(serviceInstanceId)) {
            sql.append(" and ").append(SegmentRecord.SERVICE_INSTANCE_ID).append(" = ?");
            params.add(serviceInstanceId);
        }
        if (!Strings.isNullOrEmpty(endpointId)) {
            sql.append(" and ").append(SegmentRecord.ENDPOINT_ID).append(" = ?");
            params.add(endpointId);
        }
        if (!Strings.isNullOrEmpty(traceId)) {
            sql.append(" and ").append(SegmentRecord.TRACE_ID).append(" = ?");
            params.add(traceId);
        }

        // JSON tag filter: use json_path_match on JSON column (no JOIN needed).
        // Pass the JSONPath expression as a PreparedStatement parameter to prevent injection.
        if (CollectionUtils.isNotEmpty(tags)) {
            for (final Tag tag : tags) {
                sql.append(" and json_path_match(").append(SegmentRecord.TAGS).append(", ?)");
                params.add(buildJsonPathMatchExpr(tag.getKey(), tag.getValue()));
            }
        }

        switch (traceState) {
            case ERROR:
                sql.append(" and ").append(SegmentRecord.IS_ERROR).append(" = ")
                   .append(BooleanUtils.TRUE);
                break;
            case SUCCESS:
                sql.append(" and ").append(SegmentRecord.IS_ERROR).append(" = ")
                   .append(BooleanUtils.FALSE);
                break;
            default:
                break;
        }

        switch (queryOrder) {
            case BY_START_TIME:
                sql.append(" order by ").append(SegmentRecord.START_TIME).append(" desc");
                break;
            case BY_DURATION:
                sql.append(" order by ").append(SegmentRecord.LATENCY).append(" desc");
                break;
            default:
                break;
        }

        sql.append(" limit ").append(limit);
        sql.append(" offset ").append(from);

        final List<BasicTrace> traces = new ArrayList<>();
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            setParameters(ps, params);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    final BasicTrace basicTrace = new BasicTrace();
                    basicTrace.setSegmentId(rs.getString(SegmentRecord.SEGMENT_ID));
                    basicTrace.setStart(rs.getString(SegmentRecord.START_TIME));
                    basicTrace.getEndpointNames().add(
                        IDManager.EndpointID.analysisId(
                            rs.getString(SegmentRecord.ENDPOINT_ID)).getEndpointName());
                    basicTrace.setDuration(rs.getInt(SegmentRecord.LATENCY));
                    basicTrace.setError(
                        BooleanUtils.valueToBoolean(rs.getInt(SegmentRecord.IS_ERROR)));
                    basicTrace.getTraceIds().add(rs.getString(SegmentRecord.TRACE_ID));
                    traces.add(basicTrace);
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to query basic traces", e);
        }

        return new TraceBrief(traces);
    }

    @Override
    public List<SegmentRecord> queryByTraceId(final String traceId,
                                               @Nullable final Duration duration) throws IOException {
        final String sql = DETAIL_SELECT + " from " + SegmentRecord.INDEX_NAME
            + " where " + SegmentRecord.TRACE_ID + " = ?";
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, traceId);
            try (ResultSet rs = ps.executeQuery()) {
                return buildRecords(rs);
            }
        } catch (SQLException e) {
            throw new IOException("Failed to query by trace_id: " + traceId, e);
        }
    }

    @Override
    public List<SegmentRecord> queryBySegmentIdList(final List<String> segmentIdList,
                                                     @Nullable final Duration duration) throws IOException {
        if (CollectionUtils.isEmpty(segmentIdList)) {
            return Collections.emptyList();
        }
        final String placeholders = segmentIdList.stream().map(id -> "?")
            .collect(Collectors.joining(","));
        final String sql = DETAIL_SELECT + " from " + SegmentRecord.INDEX_NAME
            + " where " + SegmentRecord.SEGMENT_ID + " in (" + placeholders + ")";
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < segmentIdList.size(); i++) {
                ps.setString(i + 1, segmentIdList.get(i));
            }
            try (ResultSet rs = ps.executeQuery()) {
                return buildRecords(rs);
            }
        } catch (SQLException e) {
            throw new IOException("Failed to query by segment ID list", e);
        }
    }

    @Override
    public List<SegmentRecord> queryByTraceIdWithInstanceId(final List<String> traceIdList,
                                                             final List<String> instanceIdList,
                                                             @Nullable final Duration duration) throws IOException {
        if (CollectionUtils.isEmpty(traceIdList)) {
            return Collections.emptyList();
        }
        final String traceIdPlaceholders = traceIdList.stream().map(id -> "?")
            .collect(Collectors.joining(","));
        final String instanceIdPlaceholders = instanceIdList.stream().map(id -> "?")
            .collect(Collectors.joining(","));
        final String sql = DETAIL_SELECT + " from " + SegmentRecord.INDEX_NAME
            + " where " + SegmentRecord.TRACE_ID + " in (" + traceIdPlaceholders + ")"
            + " and " + SegmentRecord.SERVICE_INSTANCE_ID + " in (" + instanceIdPlaceholders + ")";

        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            int idx = 1;
            for (final String tid : traceIdList) {
                ps.setString(idx++, tid);
            }
            for (final String iid : instanceIdList) {
                ps.setString(idx++, iid);
            }
            try (ResultSet rs = ps.executeQuery()) {
                return buildRecords(rs);
            }
        } catch (SQLException e) {
            throw new IOException("Failed to query by trace_id with instance_id", e);
        }
    }

    @Override
    public List<Span> doFlexibleTraceQuery(final String traceId) {
        return Collections.emptyList();
    }

    private List<SegmentRecord> buildRecords(final ResultSet rs) throws SQLException {
        final List<SegmentRecord> records = new ArrayList<>();
        while (rs.next()) {
            final SegmentRecord record = new SegmentRecord();
            record.setSegmentId(rs.getString(SegmentRecord.SEGMENT_ID));
            record.setTraceId(rs.getString(SegmentRecord.TRACE_ID));
            record.setEndpointId(rs.getString(SegmentRecord.ENDPOINT_ID));
            record.setServiceId(rs.getString(SegmentRecord.SERVICE_ID));
            record.setServiceInstanceId(rs.getString(SegmentRecord.SERVICE_INSTANCE_ID));
            record.setStartTime(rs.getLong(SegmentRecord.START_TIME));
            record.setLatency(rs.getInt(SegmentRecord.LATENCY));
            record.setIsError(rs.getInt(SegmentRecord.IS_ERROR));
            final byte[] dataBinary = rs.getBytes(SegmentRecord.DATA_BINARY);
            if (dataBinary != null) {
                record.setDataBinary(dataBinary);
            }
            records.add(record);
        }
        return records;
    }
}
