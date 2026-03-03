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
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.skywalking.oap.server.core.analysis.IDManager;
import org.apache.skywalking.oap.server.core.analysis.manual.log.LogRecord;
import org.apache.skywalking.oap.server.core.analysis.manual.searchtag.Tag;
import org.apache.skywalking.oap.server.core.query.enumeration.Order;
import org.apache.skywalking.oap.server.core.query.input.Duration;
import org.apache.skywalking.oap.server.core.query.input.TraceScopeCondition;
import org.apache.skywalking.oap.server.core.query.type.ContentType;
import org.apache.skywalking.oap.server.core.query.type.Log;
import org.apache.skywalking.oap.server.core.query.type.Logs;
import org.apache.skywalking.oap.server.core.storage.query.ILogQueryDAO;
import org.apache.skywalking.oap.server.library.util.CollectionUtils;
import org.apache.skywalking.oap.server.library.util.StringUtil;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBStorageClient;

import static java.util.Objects.nonNull;
import static org.apache.skywalking.oap.server.core.analysis.manual.log.AbstractLogRecord.CONTENT;
import static org.apache.skywalking.oap.server.core.analysis.manual.log.AbstractLogRecord.CONTENT_TYPE;
import static org.apache.skywalking.oap.server.core.analysis.manual.log.AbstractLogRecord.ENDPOINT_ID;
import static org.apache.skywalking.oap.server.core.analysis.manual.log.AbstractLogRecord.SERVICE_ID;
import static org.apache.skywalking.oap.server.core.analysis.manual.log.AbstractLogRecord.SERVICE_INSTANCE_ID;
import static org.apache.skywalking.oap.server.core.analysis.manual.log.AbstractLogRecord.SPAN_ID;
import static org.apache.skywalking.oap.server.core.analysis.manual.log.AbstractLogRecord.TAGS;
import static org.apache.skywalking.oap.server.core.analysis.manual.log.AbstractLogRecord.TAGS_RAW_DATA;
import static org.apache.skywalking.oap.server.core.analysis.manual.log.AbstractLogRecord.TIMESTAMP;
import static org.apache.skywalking.oap.server.core.analysis.manual.log.AbstractLogRecord.TRACE_ID;
import static org.apache.skywalking.oap.server.core.analysis.manual.log.AbstractLogRecord.TRACE_SEGMENT_ID;
import static org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBQueryHelper.appendTimestampCondition;
import static org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBQueryHelper.buildJsonPathMatchExpr;
import static org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBQueryHelper.setParameters;

@RequiredArgsConstructor
public class GreptimeDBLogQueryDAO implements ILogQueryDAO {
    private final GreptimeDBStorageClient client;

    @Override
    public Logs queryLogs(final String serviceId,
                          final String serviceInstanceId,
                          final String endpointId,
                          final TraceScopeCondition relatedTrace,
                          final Order queryOrder,
                          final int from,
                          final int limit,
                          final Duration duration,
                          final List<Tag> tags,
                          final List<String> keywordsOfContent,
                          final List<String> excludingKeywordsOfContent) throws IOException {
        final StringBuilder sql = new StringBuilder();
        final List<Object> params = new ArrayList<>();

        sql.append("select * from ").append(LogRecord.INDEX_NAME);
        sql.append(" where 1=1");

        if (nonNull(duration)) {
            final long startSecondTB = duration.getStartTimeBucketInSec();
            final long endSecondTB = duration.getEndTimeBucketInSec();
            if (startSecondTB != 0 && endSecondTB != 0) {
                appendTimestampCondition(sql, params, startSecondTB, endSecondTB);
            }
        }
        if (StringUtil.isNotEmpty(serviceId)) {
            sql.append(" and ").append(SERVICE_ID).append(" = ?");
            params.add(serviceId);
        }
        if (StringUtil.isNotEmpty(serviceInstanceId)) {
            sql.append(" and ").append(SERVICE_INSTANCE_ID).append(" = ?");
            params.add(serviceInstanceId);
        }
        if (StringUtil.isNotEmpty(endpointId)) {
            sql.append(" and ").append(ENDPOINT_ID).append(" = ?");
            params.add(endpointId);
        }
        if (nonNull(relatedTrace)) {
            if (StringUtil.isNotEmpty(relatedTrace.getTraceId())) {
                sql.append(" and ").append(TRACE_ID).append(" = ?");
                params.add(relatedTrace.getTraceId());
            }
            if (StringUtil.isNotEmpty(relatedTrace.getSegmentId())) {
                sql.append(" and ").append(TRACE_SEGMENT_ID).append(" = ?");
                params.add(relatedTrace.getSegmentId());
            }
            if (nonNull(relatedTrace.getSpanId())) {
                sql.append(" and ").append(SPAN_ID).append(" = ?");
                params.add(relatedTrace.getSpanId());
            }
        }

        // Pass the JSONPath expression as a PreparedStatement parameter to prevent injection.
        if (CollectionUtils.isNotEmpty(tags)) {
            for (final Tag tag : tags) {
                sql.append(" and json_path_match(").append(TAGS).append(", ?)");
                params.add(buildJsonPathMatchExpr(tag.getKey(), tag.getValue()));
            }
        }

        sql.append(" order by ").append(TIMESTAMP).append(" ")
           .append(Order.DES.equals(queryOrder) ? "desc" : "asc");
        sql.append(" limit ").append(from + limit);

        final List<Log> logs = new ArrayList<>();
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            setParameters(ps, params);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    final Log log = new Log();
                    log.setServiceId(rs.getString(SERVICE_ID));
                    log.setServiceInstanceId(rs.getString(SERVICE_INSTANCE_ID));
                    log.setEndpointId(rs.getString(ENDPOINT_ID));
                    if (log.getEndpointId() != null) {
                        log.setEndpointName(
                            IDManager.EndpointID.analysisId(log.getEndpointId()).getEndpointName());
                    }
                    log.setTraceId(rs.getString(TRACE_ID));
                    log.setTimestamp(rs.getLong(TIMESTAMP));
                    log.setContentType(ContentType.instanceOf(rs.getInt(CONTENT_TYPE)));
                    log.setContent(rs.getString(CONTENT));
                    final byte[] tagsRawData = rs.getBytes(TAGS_RAW_DATA);
                    if (tagsRawData != null && tagsRawData.length > 0) {
                        parserDataBinary(tagsRawData, log.getTags());
                    }
                    logs.add(log);
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to query logs", e);
        }

        return new Logs(
            logs.stream()
                .skip(from)
                .limit(limit)
                .collect(Collectors.toList())
        );
    }

}
