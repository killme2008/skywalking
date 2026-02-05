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
import org.apache.skywalking.oap.server.core.analysis.Layer;
import org.apache.skywalking.oap.server.core.analysis.record.Event;
import org.apache.skywalking.oap.server.core.query.PaginationUtils;
import org.apache.skywalking.oap.server.core.query.enumeration.Order;
import org.apache.skywalking.oap.server.core.query.input.Duration;
import org.apache.skywalking.oap.server.core.query.type.event.EventQueryCondition;
import org.apache.skywalking.oap.server.core.query.type.event.EventType;
import org.apache.skywalking.oap.server.core.query.type.event.Events;
import org.apache.skywalking.oap.server.core.query.type.event.Source;
import org.apache.skywalking.oap.server.core.storage.query.IEventQueryDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBStorageClient;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Comparator.comparing;
import static java.util.Objects.isNull;
import static java.util.stream.Collectors.toList;
import static org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBQueryHelper.setParameters;

@RequiredArgsConstructor
public class GreptimeDBEventQueryDAO implements IEventQueryDAO {
    private final GreptimeDBStorageClient client;

    @Override
    public Events queryEvents(final EventQueryCondition condition) throws Exception {
        final List<Object> params = new ArrayList<>();
        final String whereClause = buildWhereClause(condition, params);

        final Order queryOrder = isNull(condition.getOrder()) ? Order.DES : condition.getOrder();
        final PaginationUtils.Page page = PaginationUtils.INSTANCE.exchange(condition.getPaging());

        final StringBuilder sql = new StringBuilder();
        sql.append("select * from ").append(Event.INDEX_NAME);
        if (!whereClause.isEmpty()) {
            sql.append(" where ").append(whereClause);
        }
        if (Order.DES.equals(queryOrder)) {
            sql.append(" order by ").append(Event.TIMESTAMP).append(" desc");
        } else {
            sql.append(" order by ").append(Event.TIMESTAMP).append(" asc");
        }
        sql.append(" limit ").append(page.getLimit() + page.getFrom());

        final List<org.apache.skywalking.oap.server.core.query.type.event.Event> events = new ArrayList<>();
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            setParameters(ps, params);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    events.add(parseResultSet(rs));
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to query events", e);
        }

        return new Events(
            events.stream()
                  .skip(page.getFrom())
                  .limit(page.getLimit())
                  .collect(toList())
        );
    }

    @Override
    public Events queryEvents(final List<EventQueryCondition> conditionList) throws Exception {
        final List<Object> params = new ArrayList<>();
        final String whereClause = conditionList.stream()
            .map(condition -> {
                final List<Object> condParams = new ArrayList<>();
                final String clause = buildWhereClause(condition, condParams);
                params.addAll(condParams);
                return clause;
            })
            .filter(clause -> !clause.isEmpty())
            .map(clause -> "(" + clause + ")")
            .collect(Collectors.joining(" or "));

        final EventQueryCondition firstCondition = conditionList.get(0);
        final Order queryOrder = isNull(firstCondition.getOrder()) ? Order.DES : firstCondition.getOrder();
        final PaginationUtils.Page page = PaginationUtils.INSTANCE.exchange(firstCondition.getPaging());

        final StringBuilder sql = new StringBuilder();
        sql.append("select * from ").append(Event.INDEX_NAME);
        if (!whereClause.isEmpty()) {
            sql.append(" where ").append(whereClause);
        }
        if (Order.DES.equals(queryOrder)) {
            sql.append(" order by ").append(Event.TIMESTAMP).append(" desc");
        } else {
            sql.append(" order by ").append(Event.TIMESTAMP).append(" asc");
        }
        sql.append(" limit ").append(page.getLimit() + page.getFrom());

        final List<org.apache.skywalking.oap.server.core.query.type.event.Event> events = new ArrayList<>();
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            setParameters(ps, params);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    events.add(parseResultSet(rs));
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to query events", e);
        }

        final var comparator = Order.DES.equals(queryOrder) ?
            comparing(org.apache.skywalking.oap.server.core.query.type.event.Event::getTimestamp).reversed() :
            comparing(org.apache.skywalking.oap.server.core.query.type.event.Event::getTimestamp);
        return new Events(
            events.stream()
                  .sorted(comparator)
                  .skip(page.getFrom())
                  .limit(page.getLimit())
                  .collect(toList())
        );
    }

    private String buildWhereClause(final EventQueryCondition condition,
                                    final List<Object> params) {
        final List<String> conditions = new ArrayList<>();

        if (!isNullOrEmpty(condition.getUuid())) {
            conditions.add(Event.UUID + " = ?");
            params.add(condition.getUuid());
        }

        final Source source = condition.getSource();
        if (source != null) {
            if (!isNullOrEmpty(source.getService())) {
                conditions.add(Event.SERVICE + " = ?");
                params.add(source.getService());
            }
            if (!isNullOrEmpty(source.getServiceInstance())) {
                conditions.add(Event.SERVICE_INSTANCE + " = ?");
                params.add(source.getServiceInstance());
            }
            if (!isNullOrEmpty(source.getEndpoint())) {
                conditions.add(Event.ENDPOINT + " = ?");
                params.add(source.getEndpoint());
            }
        }

        if (!isNullOrEmpty(condition.getName())) {
            conditions.add(Event.NAME + " = ?");
            params.add(condition.getName());
        }

        if (condition.getType() != null) {
            conditions.add(Event.TYPE + " = ?");
            params.add(condition.getType().name());
        }

        final Duration time = condition.getTime();
        if (time != null) {
            if (time.getStartTimestamp() > 0) {
                conditions.add(Event.TIMESTAMP + " > ?");
                params.add(time.getStartTimestamp());
            }
            if (time.getEndTimestamp() > 0) {
                conditions.add(Event.TIMESTAMP + " < ?");
                params.add(time.getEndTimestamp());
            }
        }

        if (!isNullOrEmpty(condition.getLayer())) {
            conditions.add(Event.LAYER + " = ?");
            params.add(Layer.nameOf(condition.getLayer()).value());
        }

        return String.join(" and ", conditions);
    }

    private org.apache.skywalking.oap.server.core.query.type.event.Event parseResultSet(
            final ResultSet rs) throws SQLException {
        final var event = new org.apache.skywalking.oap.server.core.query.type.event.Event();
        event.setUuid(rs.getString(Event.UUID));

        final String service = rs.getString(Event.SERVICE);
        final String serviceInstance = rs.getString(Event.SERVICE_INSTANCE);
        final String endpoint = rs.getString(Event.ENDPOINT);
        event.setSource(new Source(service, serviceInstance, endpoint));

        event.setName(rs.getString(Event.NAME));
        event.setType(EventType.parse(rs.getString(Event.TYPE)));
        event.setMessage(rs.getString(Event.MESSAGE));
        event.setParameters(rs.getString(Event.PARAMETERS));
        event.setStartTime(rs.getLong(Event.START_TIME));
        event.setEndTime(rs.getLong(Event.END_TIME));
        event.setTimestamp(rs.getLong(Event.TIMESTAMP));
        event.setLayer(Layer.valueOf(rs.getInt(Event.LAYER)).name());
        return event;
    }

}
