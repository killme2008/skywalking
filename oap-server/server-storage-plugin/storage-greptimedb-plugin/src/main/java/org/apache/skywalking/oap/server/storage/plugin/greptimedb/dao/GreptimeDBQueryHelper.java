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
import java.util.List;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.skywalking.oap.server.library.util.StringUtil;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBConverter;

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
                ps.setTimestamp(i + 1, (Timestamp) param);
            } else {
                ps.setObject(i + 1, param);
            }
        }
    }

    /**
     * Append {@code AND greptime_ts >= ? AND greptime_ts <= ?} to a StringBuilder query.
     * Used by most DAOs that build SQL via StringBuilder.
     */
    static void appendTimestampCondition(final StringBuilder sql, final List<Object> params,
                                         final long startTimeBucket, final long endTimeBucket) {
        sql.append(" and ").append(GREPTIME_TS).append(" >= ?");
        params.add(toTimestamp(startTimeBucket));
        sql.append(" and ").append(GREPTIME_TS).append(" <= ?");
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

    /**
     * Wrap a traffic/metadata table read so that only the latest row per synthetic {@code id}
     * is returned. These tables accumulate one row per active minute (the {@code id} is a stable
     * business key, {@code greptime_ts} increases each persistence period); a self-join against
     * max({@code greptime_ts}) per id collapses them to a single current row, matching the
     * upsert-in-place semantics of the JDBC/ES storages.
     *
     * @param table       the physical table name
     * @param innerWhere  row-selection predicate without the {@code where} keyword (null/empty for
     *                    none); every bound parameter belongs to this clause
     * @param outerSuffix an optional trailing clause applied to the deduplicated rows, e.g.
     *                    {@code order by t.`time_bucket` desc} (null/empty for none)
     * @param limit       row limit, appended only when positive
     * @return the deduplicating SQL
     */
    static String latestPerIdSql(final String table, final String innerWhere,
                                 final String outerSuffix, final int limit) {
        final String id = GreptimeDBConverter.quoteColumn("id");
        final String ts = GreptimeDBConverter.quoteColumn(GREPTIME_TS);
        final StringBuilder sql = new StringBuilder();
        sql.append("select t.* from ").append(table).append(" t join (select ")
           .append(id).append(", max(").append(ts).append(") as mx from ").append(table);
        if (StringUtil.isNotEmpty(innerWhere)) {
            sql.append(" where ").append(innerWhere);
        }
        sql.append(" group by ").append(id).append(") latest on t.").append(id)
           .append(" = latest.").append(id).append(" and t.").append(ts).append(" = latest.mx");
        if (StringUtil.isNotEmpty(outerSuffix)) {
            sql.append(' ').append(outerSuffix);
        }
        if (limit > 0) {
            sql.append(" limit ").append(limit);
        }
        return sql.toString();
    }

    static String buildJsonPathMatchExpr(final String key, final String value) {
        return "$[\"" + escapeJsonPath(key) + "\"] == \"" + escapeJsonPath(value) + "\"";
    }

    private static String escapeJsonPath(final String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
