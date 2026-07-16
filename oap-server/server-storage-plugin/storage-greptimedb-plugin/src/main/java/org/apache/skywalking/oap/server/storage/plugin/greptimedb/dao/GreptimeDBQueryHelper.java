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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.skywalking.oap.server.library.util.StringUtil;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBConverter;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBTableSchema;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class GreptimeDBQueryHelper {

    /**
     * The GreptimeDB TIME INDEX column name used across all tables.
     * Queries should filter on this column instead of {@code time_bucket}
     * to leverage native partition pruning rather than secondary index lookups.
     */
    static final String GREPTIME_TS = "greptime_ts";

    /**
     * Convert a SkyWalking time_bucket value to a JDBC Timestamp for use
     * in {@code greptime_ts} range predicates.
     */
    static Timestamp toTimestamp(final long timeBucket) {
        return new Timestamp(GreptimeDBConverter.timeBucketToTimestamp(timeBucket));
    }

    static void setParameters(final PreparedStatement ps,
                              final List<Object> params) throws SQLException {
        for (int i = 0; i < params.size(); i++) {
            final Object param = params.get(i);
            if (param instanceof Long) {
                ps.setLong(i + 1, (Long) param);
            } else if (param instanceof Integer) {
                ps.setInt(i + 1, (Integer) param);
            } else if (param instanceof String) {
                ps.setString(i + 1, (String) param);
            } else if (param instanceof Timestamp) {
                setTimestamp(ps, i + 1, (Timestamp) param);
            } else {
                ps.setObject(i + 1, param);
            }
        }
    }

    static void setTimestamp(final PreparedStatement ps,
                             final int index,
                             final Timestamp timestamp) throws SQLException {
        ps.setTimestamp(index, timestamp, Calendar.getInstance(TimeZone.getTimeZone("UTC")));
    }

    /**
     * Append {@code AND greptime_ts >= ? AND greptime_ts <= ?} to a StringBuilder query.
     * Used by most DAOs that build SQL via StringBuilder.
     */
    static void appendTimestampCondition(final StringBuilder sql, final List<Object> params,
                                         final long startTimeBucket, final long endTimeBucket) {
        appendTimestampCondition(sql, params, null, startTimeBucket, endTimeBucket);
    }

    static void appendTimestampCondition(final StringBuilder sql, final List<Object> params,
                                         final String alias,
                                         final long startTimeBucket, final long endTimeBucket) {
        final String prefix = alias == null ? "" : alias + '.';
        sql.append(" and ").append(prefix).append(GREPTIME_TS).append(" >= ?");
        params.add(toTimestamp(startTimeBucket));
        sql.append(" and ").append(prefix).append(GREPTIME_TS).append(" <= ?");
        params.add(toTimestamp(endTimeBucket));
    }

    /**
     * Add {@code greptime_ts >= ?} / {@code greptime_ts <= ?} to a conditions list.
     * Used by task DAOs that collect conditions before joining them.
     */
    static void addTimestampConditions(final List<String> conditions, final List<Object> params,
                                       final Long startTimeBucket, final Long endTimeBucket) {
        if (startTimeBucket != null) {
            conditions.add(GREPTIME_TS + " >= ?");
            params.add(toTimestamp(startTimeBucket));
        }
        if (endTimeBucket != null) {
            conditions.add(GREPTIME_TS + " <= ?");
            params.add(toTimestamp(endTimeBucket));
        }
    }

    static String latestPerSeriesSql(final GreptimeDBTableSchema schema,
                                     final String innerWhere,
                                     final String outerSuffix,
                                     final int limit) {
        if (schema.getPrimaryKeys().isEmpty()) {
            throw new IllegalArgumentException(
                "Latest-row query requires a primary key: " + schema.getTableName());
        }
        final String ts = GreptimeDBConverter.quoteColumn(GREPTIME_TS);
        final StringBuilder sql = new StringBuilder();
        sql.append("select ");
        for (int i = 0; i < schema.getColumns().size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            final String column = GreptimeDBConverter.quoteColumn(
                schema.getColumns().get(i).getName());
            if (schema.getPrimaryKeys().contains(schema.getColumns().get(i).getName())) {
                sql.append(column);
            } else {
                sql.append("last_value(").append(column).append(" order by ").append(ts)
                    .append(") as ").append(column);
            }
        }
        sql.append(" from ").append(schema.getTableName());
        if (StringUtil.isNotEmpty(innerWhere)) {
            sql.append(" where ").append(innerWhere);
        }
        sql.append(" group by ").append(schema.getPrimaryKeys().stream()
            .map(GreptimeDBConverter::quoteColumn)
            .collect(java.util.stream.Collectors.joining(", ")));
        if (StringUtil.isNotEmpty(outerSuffix)) {
            sql.append(' ').append(outerSuffix);
        }
        if (limit > 0) {
            sql.append(" limit ").append(limit);
        }
        return sql.toString();
    }

    static void appendAdditionalEntityConditions(final StringBuilder sql,
                                                  final List<Object> params,
                                                  final String mainAlias,
                                                  final String table,
                                                  final String valueColumn,
                                                  final List<String> values,
                                                  final Timestamp start,
                                                  final Timestamp end) {
        for (int i = 0; i < values.size(); i++) {
            final String alias = "tag_" + i;
            sql.append(" and exists (select 1 from ").append(table).append(' ').append(alias)
                .append(" where ").append(alias).append(".`id` = ").append(mainAlias).append(".`id`")
                .append(" and ").append(alias).append(".`greptime_ts` = ")
                .append(mainAlias).append(".`greptime_ts`")
                .append(" and ").append(alias).append('.')
                .append(GreptimeDBConverter.quoteColumn(valueColumn)).append(" = ?");
            params.add(values.get(i));
            if (start != null && end != null) {
                sql.append(" and ").append(alias).append(".`greptime_ts` >= ?")
                    .append(" and ").append(alias).append(".`greptime_ts` <= ?");
                params.add(start);
                params.add(end);
            }
            sql.append(')');
        }
    }
}
