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
import org.apache.skywalking.oap.server.core.browser.manual.errorlog.BrowserErrorLogRecord;
import org.apache.skywalking.oap.server.core.browser.source.BrowserErrorCategory;
import org.apache.skywalking.oap.server.core.query.input.Duration;
import org.apache.skywalking.oap.server.core.query.type.BrowserErrorLog;
import org.apache.skywalking.oap.server.core.query.type.BrowserErrorLogs;
import org.apache.skywalking.oap.server.core.storage.query.IBrowserLogQueryDAO;
import org.apache.skywalking.oap.server.library.util.StringUtil;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBStorageClient;

import static java.util.Comparator.comparing;
import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBQueryHelper.appendTimestampCondition;
import static org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBQueryHelper.setParameters;

@RequiredArgsConstructor
public class GreptimeDBBrowserLogQueryDAO implements IBrowserLogQueryDAO {
    private final GreptimeDBStorageClient client;

    @Override
    public BrowserErrorLogs queryBrowserErrorLogs(final String serviceId,
                                                   final String serviceVersionId,
                                                   final String pagePathId,
                                                   final BrowserErrorCategory category,
                                                   final Duration duration,
                                                   final int limit,
                                                   final int from) throws IOException {
        final StringBuilder sql = new StringBuilder();
        final List<Object> params = new ArrayList<>();

        sql.append("select ").append(BrowserErrorLogRecord.DATA_BINARY)
           .append(" from ").append(BrowserErrorLogRecord.INDEX_NAME)
           .append(" where 1=1");

        if (nonNull(duration)) {
            final long startSecondTB = duration.getStartTimeBucketInSec();
            final long endSecondTB = duration.getEndTimeBucketInSec();
            if (startSecondTB != 0 && endSecondTB != 0) {
                appendTimestampCondition(sql, params, startSecondTB, endSecondTB);
            }
        }
        if (StringUtil.isNotEmpty(serviceId)) {
            sql.append(" and ").append(BrowserErrorLogRecord.SERVICE_ID).append(" = ?");
            params.add(serviceId);
        }
        if (StringUtil.isNotEmpty(serviceVersionId)) {
            sql.append(" and ").append(BrowserErrorLogRecord.SERVICE_VERSION_ID).append(" = ?");
            params.add(serviceVersionId);
        }
        if (StringUtil.isNotEmpty(pagePathId)) {
            sql.append(" and ").append(BrowserErrorLogRecord.PAGE_PATH_ID).append(" = ?");
            params.add(pagePathId);
        }
        if (nonNull(category)) {
            sql.append(" and ").append(BrowserErrorLogRecord.ERROR_CATEGORY).append(" = ?");
            params.add(category.getValue());
        }

        sql.append(" order by ").append(BrowserErrorLogRecord.TIMESTAMP).append(" desc");
        sql.append(" limit ").append(from + limit);

        final List<BrowserErrorLog> logs = new ArrayList<>();
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            setParameters(ps, params);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    final byte[] dataBinary = rs.getBytes(BrowserErrorLogRecord.DATA_BINARY);
                    if (dataBinary != null && dataBinary.length > 0) {
                        logs.add(parserDataBinary(dataBinary));
                    }
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to query browser error logs", e);
        }

        return new BrowserErrorLogs(
            logs.stream()
                .sorted(comparing(BrowserErrorLog::getTime).reversed())
                .skip(from)
                .limit(limit)
                .collect(toList())
        );
    }

}
