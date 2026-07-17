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
import java.util.Set;
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
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBConverter;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBSearchableTagColumns;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBStorageClient;

import static java.util.Objects.nonNull;
import static org.apache.skywalking.oap.server.core.analysis.manual.log.AbstractLogRecord.CONTENT;
import static org.apache.skywalking.oap.server.core.analysis.manual.log.AbstractLogRecord.CONTENT_TYPE;
import static org.apache.skywalking.oap.server.core.analysis.manual.log.AbstractLogRecord.ENDPOINT_ID;
import static org.apache.skywalking.oap.server.core.analysis.manual.log.AbstractLogRecord.SERVICE_ID;
import static org.apache.skywalking.oap.server.core.analysis.manual.log.AbstractLogRecord.SERVICE_INSTANCE_ID;
import static org.apache.skywalking.oap.server.core.analysis.manual.log.AbstractLogRecord.SPAN_ID;
import static org.apache.skywalking.oap.server.core.analysis.manual.log.AbstractLogRecord.TAGS_RAW_DATA;
import static org.apache.skywalking.oap.server.core.analysis.manual.log.AbstractLogRecord.TIMESTAMP;
import static org.apache.skywalking.oap.server.core.analysis.manual.log.AbstractLogRecord.TRACE_ID;
import static org.apache.skywalking.oap.server.core.analysis.manual.log.AbstractLogRecord.TRACE_SEGMENT_ID;
import static org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBQueryHelper.appendTimestampCondition;
import static org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBQueryHelper.appendAdditionalEntityConditions;
import static org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBQueryHelper.setParameters;
import static org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBQueryHelper.toTimestamp;

@RequiredArgsConstructor
public class GreptimeDBLogQueryDAO implements ILogQueryDAO {
    private final GreptimeDBStorageClient client;
    private final GreptimeDBSearchableTagColumns tagColumns;

    @Override
    public boolean supportQueryLogsByKeywords() {
        // content is FULLTEXT-indexed, so keyword search is served via matches_term.
        return true;
    }

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

        final String main = "l";
        sql.append("select ").append(main).append(".* from ")
            .append(LogRecord.INDEX_NAME).append(' ').append(main);
        boolean searchableTags = true;
        if (CollectionUtils.isNotEmpty(tags)) {
            final Set<String> searchable = tagColumns.searchableKeys(LogRecord.INDEX_NAME);
            searchableTags = tags.stream().allMatch(tag -> searchable.contains(tag.getKey()));
        }
        sql.append(" where 1=1");
        if (!searchableTags) {
            sql.append(" and 1=0");
        }

        java.sql.Timestamp tagStart = null;
        java.sql.Timestamp tagEnd = null;
        if (nonNull(duration)) {
            final long startSecondTB = duration.getStartTimeBucketInSec();
            final long endSecondTB = duration.getEndTimeBucketInSec();
            if (startSecondTB != 0 && endSecondTB != 0) {
                appendTimestampCondition(sql, params, main, startSecondTB, endSecondTB);
                tagStart = toTimestamp(startSecondTB);
                tagEnd = toTimestamp(endSecondTB);
            }
        }
        if (searchableTags && CollectionUtils.isNotEmpty(tags)) {
            appendAdditionalEntityConditions(
                sql, params, main, LogRecord.ADDITIONAL_TAG_TABLE, LogRecord.TAGS,
                tags.stream().map(Tag::toString).collect(Collectors.toList()), tagStart, tagEnd);
        }
        if (StringUtil.isNotEmpty(serviceId)) {
            sql.append(" and ").append(main).append('.').append(SERVICE_ID).append(" = ?");
            params.add(serviceId);
        }
        if (StringUtil.isNotEmpty(serviceInstanceId)) {
            sql.append(" and ").append(main).append('.').append(SERVICE_INSTANCE_ID).append(" = ?");
            params.add(serviceInstanceId);
        }
        if (StringUtil.isNotEmpty(endpointId)) {
            sql.append(" and ").append(main).append('.').append(ENDPOINT_ID).append(" = ?");
            params.add(endpointId);
        }
        if (nonNull(relatedTrace)) {
            if (StringUtil.isNotEmpty(relatedTrace.getTraceId())) {
                sql.append(" and ").append(main).append('.').append(TRACE_ID).append(" = ?");
                params.add(relatedTrace.getTraceId());
            }
            if (StringUtil.isNotEmpty(relatedTrace.getSegmentId())) {
                sql.append(" and ").append(main).append('.').append(TRACE_SEGMENT_ID).append(" = ?");
                params.add(relatedTrace.getSegmentId());
            }
            if (nonNull(relatedTrace.getSpanId())) {
                sql.append(" and ").append(main).append('.').append(SPAN_ID).append(" = ?");
                params.add(relatedTrace.getSpanId());
            }
        }

        // Content keyword search over the FULLTEXT-indexed content column. matches_term is exact
        // word-level matching; lower() on both sides makes it case-insensitive (the FULLTEXT index's
        // case_sensitive option only affects the matches() query path, not matches_term).
        final String contentColumn = main + "." + GreptimeDBConverter.quoteColumn(CONTENT);
        if (CollectionUtils.isNotEmpty(keywordsOfContent)) {
            for (final String keyword : keywordsOfContent) {
                sql.append(" and matches_term(lower(").append(contentColumn).append("), lower(?))");
                params.add(keyword);
            }
        }
        if (CollectionUtils.isNotEmpty(excludingKeywordsOfContent)) {
            for (final String keyword : excludingKeywordsOfContent) {
                sql.append(" and not matches_term(lower(").append(contentColumn).append("), lower(?))");
                params.add(keyword);
            }
        }

        sql.append(" order by ").append(main).append('.').append(TIMESTAMP).append(" ")
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
