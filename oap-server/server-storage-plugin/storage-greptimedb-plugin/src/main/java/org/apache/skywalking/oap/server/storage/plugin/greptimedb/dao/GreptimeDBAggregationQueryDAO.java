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
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.skywalking.oap.server.core.analysis.metrics.Metrics;
import org.apache.skywalking.oap.server.core.query.enumeration.Order;
import org.apache.skywalking.oap.server.core.query.input.Duration;
import org.apache.skywalking.oap.server.core.query.input.TopNCondition;
import org.apache.skywalking.oap.server.core.query.type.KeyValue;
import org.apache.skywalking.oap.server.core.query.type.SelectedRecord;
import org.apache.skywalking.oap.server.core.storage.query.IAggregationQueryDAO;
import org.apache.skywalking.oap.server.library.util.CollectionUtils;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBConverter;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBStorageClient;

import static org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBQueryHelper.appendTimestampCondition;
import static org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBQueryHelper.setParameters;

@RequiredArgsConstructor
public class GreptimeDBAggregationQueryDAO implements IAggregationQueryDAO {
    private final GreptimeDBStorageClient client;

    @Override
    public List<SelectedRecord> sortMetrics(final TopNCondition condition,
                                             final String valueColumnName,
                                             final Duration duration,
                                             final List<KeyValue> additionalConditions) throws IOException {
        final String tableName = GreptimeDBConverter.resolveMetricsTableName(
            condition.getName(), duration.getStep());

        final StringBuilder sql = new StringBuilder();
        final List<Object> params = new ArrayList<>();

        // Subquery: aggregate by entity_id
        sql.append("select result, ").append(Metrics.ENTITY_ID);
        sql.append(" from (select avg(").append(valueColumnName).append(") as result, ");
        sql.append(Metrics.ENTITY_ID);
        sql.append(" from ").append(tableName);
        sql.append(" where 1=1");
        appendTimestampCondition(sql, params, duration.getStartTimeBucket(), duration.getEndTimeBucket());

        if (additionalConditions != null) {
            for (final KeyValue kv : additionalConditions) {
                sql.append(" and ").append(kv.getKey()).append(" = ?");
                params.add(kv.getValue());
            }
        }
        if (CollectionUtils.isNotEmpty(condition.getAttributes())) {
            for (final var attr : condition.getAttributes()) {
                if (attr.isEquals()) {
                    sql.append(" and ").append(attr.getKey()).append(" = ?");
                } else {
                    // NULL-safe not-equals: NULL != 'value' evaluates to NULL in SQL,
                    // so rows with NULL attr would be excluded. Use IS NULL OR != instead.
                    sql.append(" and (").append(attr.getKey()).append(" IS NULL OR ")
                       .append(attr.getKey()).append(" != ?)");
                }
                params.add(attr.getValue());
            }
        }

        sql.append(" group by ").append(Metrics.ENTITY_ID);
        sql.append(") as T order by result");
        sql.append(Order.ASC.equals(condition.getOrder()) ? " asc" : " desc");
        sql.append(" limit ").append(condition.getTopN());

        final List<SelectedRecord> results = new ArrayList<>();
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            setParameters(ps, params);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    final SelectedRecord record = new SelectedRecord();
                    record.setId(rs.getString(Metrics.ENTITY_ID));
                    record.setValue(String.valueOf(rs.getInt("result")));
                    results.add(record);
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to sort metrics from " + tableName, e);
        }

        final Comparator<SelectedRecord> comparator = Order.ASC.equals(condition.getOrder())
            ? Comparator.comparingLong(it -> Long.parseLong(it.getValue()))
            : Comparator.<SelectedRecord, Long>comparing(
                it -> Long.parseLong(it.getValue())).reversed();
        return results.stream().sorted(comparator)
            .limit(condition.getTopN())
            .collect(Collectors.toList());
    }
}
