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
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import org.apache.skywalking.oap.server.core.analysis.manual.spanattach.SWSpanAttachedEventRecord;
import org.apache.skywalking.oap.server.core.analysis.manual.spanattach.SpanAttachedEventRecord;
import org.apache.skywalking.oap.server.core.query.input.Duration;
import org.apache.skywalking.oap.server.core.storage.query.ISpanAttachedEventQueryDAO;
import org.apache.skywalking.oap.server.library.util.StringUtil;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBStorageClient;

@RequiredArgsConstructor
public class GreptimeDBSpanAttachedEventQueryDAO implements ISpanAttachedEventQueryDAO {
    private final GreptimeDBStorageClient client;

    @Override
    public List<SWSpanAttachedEventRecord> querySWSpanAttachedEvents(
            final List<String> traceIds,
            @Nullable final Duration duration) throws IOException {
        if (traceIds == null || traceIds.isEmpty()) {
            return new ArrayList<>();
        }
        final String placeholders = traceIds.stream().map(id -> "?")
            .collect(Collectors.joining(","));
        final String sql = "select * from " + SWSpanAttachedEventRecord.INDEX_NAME
            + " where " + SWSpanAttachedEventRecord.RELATED_TRACE_ID + " in (" + placeholders + ")"
            + " order by " + SWSpanAttachedEventRecord.START_TIME_SECOND
            + ", " + SWSpanAttachedEventRecord.START_TIME_NANOS + " asc";

        final List<SWSpanAttachedEventRecord> results = new ArrayList<>();
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < traceIds.size(); i++) {
                ps.setString(i + 1, traceIds.get(i));
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    final SWSpanAttachedEventRecord record = new SWSpanAttachedEventRecord();
                    record.setStartTimeSecond(rs.getLong(SWSpanAttachedEventRecord.START_TIME_SECOND));
                    record.setStartTimeNanos(rs.getInt(SWSpanAttachedEventRecord.START_TIME_NANOS));
                    record.setEvent(rs.getString(SWSpanAttachedEventRecord.EVENT));
                    record.setEndTimeSecond(rs.getLong(SWSpanAttachedEventRecord.END_TIME_SECOND));
                    record.setEndTimeNanos(rs.getInt(SWSpanAttachedEventRecord.END_TIME_NANOS));
                    record.setTraceRefType(rs.getInt(SWSpanAttachedEventRecord.TRACE_REF_TYPE));
                    record.setRelatedTraceId(rs.getString(SWSpanAttachedEventRecord.RELATED_TRACE_ID));
                    record.setTraceSegmentId(rs.getString(SWSpanAttachedEventRecord.TRACE_SEGMENT_ID));
                    record.setTraceSpanId(rs.getString(SWSpanAttachedEventRecord.TRACE_SPAN_ID));
                    final String dataBinaryBase64 = rs.getString(SWSpanAttachedEventRecord.DATA_BINARY);
                    if (StringUtil.isNotEmpty(dataBinaryBase64)) {
                        record.setDataBinary(Base64.getDecoder().decode(dataBinaryBase64));
                    }
                    results.add(record);
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to query SW span attached events", e);
        }
        return results.stream()
            .sorted(Comparator.comparing(SWSpanAttachedEventRecord::getStartTimeSecond)
                               .thenComparing(SWSpanAttachedEventRecord::getStartTimeNanos))
            .collect(Collectors.toList());
    }

    @Override
    public List<SpanAttachedEventRecord> queryZKSpanAttachedEvents(
            final List<String> traceIds,
            @Nullable final Duration duration) throws IOException {
        if (traceIds == null || traceIds.isEmpty()) {
            return new ArrayList<>();
        }
        final String placeholders = traceIds.stream().map(id -> "?")
            .collect(Collectors.joining(","));
        final String sql = "select * from " + SpanAttachedEventRecord.INDEX_NAME
            + " where " + SpanAttachedEventRecord.RELATED_TRACE_ID + " in (" + placeholders + ")"
            + " order by " + SpanAttachedEventRecord.START_TIME_SECOND
            + ", " + SpanAttachedEventRecord.START_TIME_NANOS + " asc";

        final List<SpanAttachedEventRecord> results = new ArrayList<>();
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < traceIds.size(); i++) {
                ps.setString(i + 1, traceIds.get(i));
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    final SpanAttachedEventRecord record = new SpanAttachedEventRecord();
                    record.setStartTimeSecond(rs.getLong(SpanAttachedEventRecord.START_TIME_SECOND));
                    record.setStartTimeNanos(rs.getInt(SpanAttachedEventRecord.START_TIME_NANOS));
                    record.setEvent(rs.getString(SpanAttachedEventRecord.EVENT));
                    record.setEndTimeSecond(rs.getLong(SpanAttachedEventRecord.END_TIME_SECOND));
                    record.setEndTimeNanos(rs.getInt(SpanAttachedEventRecord.END_TIME_NANOS));
                    record.setTraceRefType(rs.getInt(SpanAttachedEventRecord.TRACE_REF_TYPE));
                    record.setRelatedTraceId(rs.getString(SpanAttachedEventRecord.RELATED_TRACE_ID));
                    record.setTraceSegmentId(rs.getString(SpanAttachedEventRecord.TRACE_SEGMENT_ID));
                    record.setTraceSpanId(rs.getString(SpanAttachedEventRecord.TRACE_SPAN_ID));
                    final String dataBinaryBase64 = rs.getString(SpanAttachedEventRecord.DATA_BINARY);
                    if (StringUtil.isNotEmpty(dataBinaryBase64)) {
                        record.setDataBinary(Base64.getDecoder().decode(dataBinaryBase64));
                    }
                    results.add(record);
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to query ZK span attached events", e);
        }
        return results.stream()
            .sorted(Comparator.comparing(SpanAttachedEventRecord::getStartTimeSecond)
                               .thenComparing(SpanAttachedEventRecord::getStartTimeNanos))
            .collect(Collectors.toList());
    }
}
