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
import org.apache.skywalking.oap.server.core.analysis.topn.TopN;
import org.apache.skywalking.oap.server.core.query.enumeration.Order;
import org.apache.skywalking.oap.server.core.query.input.Duration;
import org.apache.skywalking.oap.server.core.query.input.RecordCondition;
import org.apache.skywalking.oap.server.core.query.type.Record;
import org.apache.skywalking.oap.server.core.storage.query.IRecordsQueryDAO;
import org.apache.skywalking.oap.server.library.util.StringUtil;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBStorageClient;

@RequiredArgsConstructor
public class GreptimeDBRecordsQueryDAO implements IRecordsQueryDAO {
    private final GreptimeDBStorageClient client;

    @Override
    public List<Record> readRecords(final RecordCondition condition,
                                     final String valueColumnName,
                                     final Duration duration) throws IOException {
        // TopN records: table name is the record name directly (no downsampling suffix)
        final String tableName = condition.getName();
        final StringBuilder sql = new StringBuilder();
        final List<Object> params = new ArrayList<>();

        sql.append("select * from ").append(tableName);
        sql.append(" where ").append(TopN.ENTITY_ID).append(" = ?");
        params.add(condition.getParentEntity().buildId());
        sql.append(" and ").append(TopN.TIME_BUCKET).append(" >= ?");
        params.add(duration.getStartTimeBucketInSec());
        sql.append(" and ").append(TopN.TIME_BUCKET).append(" <= ?");
        params.add(duration.getEndTimeBucketInSec());

        sql.append(" order by ").append(valueColumnName);
        if (condition.getOrder().equals(Order.DES)) {
            sql.append(" desc");
        } else {
            sql.append(" asc");
        }
        sql.append(" limit ").append(condition.getTopN());

        final List<Record> results = new ArrayList<>();
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) {
                final Object param = params.get(i);
                if (param instanceof Long) {
                    ps.setLong(i + 1, (Long) param);
                } else if (param instanceof String) {
                    ps.setString(i + 1, (String) param);
                } else {
                    ps.setObject(i + 1, param);
                }
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    final Record record = new Record();
                    record.setName(rs.getString(TopN.STATEMENT));
                    final String refId = rs.getString(TopN.TRACE_ID);
                    record.setRefId(StringUtil.isEmpty(refId) ? "" : refId);
                    record.setId(record.getRefId());
                    record.setValue(String.valueOf(rs.getInt(valueColumnName)));
                    results.add(record);
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to read records from " + tableName, e);
        }

        return results;
    }
}
