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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.skywalking.oap.server.core.analysis.TimeBucket;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;

class GreptimeDBQueryHelperTest {

    // ---- toTimestamp ----

    @Test
    void toTimestampShouldConvertMinuteBucket() {
        // 202401011200 -> 2024-01-01 12:00 UTC
        final long minuteBucket = 202401011200L;
        final Timestamp ts = GreptimeDBQueryHelper.toTimestamp(minuteBucket);
        assertEquals(TimeBucket.getTimestamp(minuteBucket), ts.getTime());
    }

    @Test
    void toTimestampShouldConvertSecondBucket() {
        // 20240101120030 -> 2024-01-01 12:00:30 UTC
        final long secondBucket = 20240101120030L;
        final Timestamp ts = GreptimeDBQueryHelper.toTimestamp(secondBucket);
        assertEquals(TimeBucket.getTimestamp(secondBucket), ts.getTime());
    }

    @Test
    void toTimestampShouldConvertDayBucket() {
        // 20240101 -> 2024-01-01 00:00 UTC
        final long dayBucket = 20240101L;
        final Timestamp ts = GreptimeDBQueryHelper.toTimestamp(dayBucket);
        assertEquals(TimeBucket.getTimestamp(dayBucket), ts.getTime());
    }

    @Test
    void toTimestampShouldConvertHourBucket() {
        // 2024010112 -> 2024-01-01 12:00 UTC
        final long hourBucket = 2024010112L;
        final Timestamp ts = GreptimeDBQueryHelper.toTimestamp(hourBucket);
        assertEquals(TimeBucket.getTimestamp(hourBucket), ts.getTime());
    }

    // ---- appendTimestampCondition ----

    @Test
    void appendTimestampConditionShouldAppendBothBounds() {
        final StringBuilder sql = new StringBuilder("select * from t where 1=1");
        final List<Object> params = new ArrayList<>();

        final long startTB = 202401010000L;
        final long endTB = 202401012359L;
        GreptimeDBQueryHelper.appendTimestampCondition(sql, params, startTB, endTB);

        final String result = sql.toString();
        assertTrue(result.contains(" and greptime_ts >= ?"));
        assertTrue(result.contains(" and greptime_ts <= ?"));
        assertEquals(2, params.size());
        assertTrue(params.get(0) instanceof Timestamp);
        assertTrue(params.get(1) instanceof Timestamp);
        assertEquals(TimeBucket.getTimestamp(startTB), ((Timestamp) params.get(0)).getTime());
        assertEquals(TimeBucket.getTimestamp(endTB), ((Timestamp) params.get(1)).getTime());
    }

    // ---- addTimestampConditions ----

    @Test
    void addTimestampConditionsShouldAddBothWhenPresent() {
        final List<String> conditions = new ArrayList<>();
        final List<Object> params = new ArrayList<>();

        final long startTB = 202401010000L;
        final long endTB = 202401012359L;
        GreptimeDBQueryHelper.addTimestampConditions(conditions, params, startTB, endTB);

        assertEquals(2, conditions.size());
        assertEquals("greptime_ts >= ?", conditions.get(0));
        assertEquals("greptime_ts <= ?", conditions.get(1));
        assertEquals(2, params.size());
        assertEquals(TimeBucket.getTimestamp(startTB), ((Timestamp) params.get(0)).getTime());
        assertEquals(TimeBucket.getTimestamp(endTB), ((Timestamp) params.get(1)).getTime());
    }

    @Test
    void addTimestampConditionsShouldSkipNullStart() {
        final List<String> conditions = new ArrayList<>();
        final List<Object> params = new ArrayList<>();

        GreptimeDBQueryHelper.addTimestampConditions(conditions, params, null, 202401012359L);

        assertEquals(1, conditions.size());
        assertEquals("greptime_ts <= ?", conditions.get(0));
        assertEquals(1, params.size());
    }

    @Test
    void addTimestampConditionsShouldSkipNullEnd() {
        final List<String> conditions = new ArrayList<>();
        final List<Object> params = new ArrayList<>();

        GreptimeDBQueryHelper.addTimestampConditions(conditions, params, 202401010000L, null);

        assertEquals(1, conditions.size());
        assertEquals("greptime_ts >= ?", conditions.get(0));
        assertEquals(1, params.size());
    }

    @Test
    void addTimestampConditionsShouldSkipBothNull() {
        final List<String> conditions = new ArrayList<>();
        final List<Object> params = new ArrayList<>();

        GreptimeDBQueryHelper.addTimestampConditions(conditions, params, null, null);

        assertTrue(conditions.isEmpty());
        assertTrue(params.isEmpty());
    }

    // ---- setParameters ----

    @Test
    void setParametersShouldHandleTimestamp() throws SQLException {
        final PreparedStatement ps = Mockito.mock(PreparedStatement.class);
        final Timestamp ts = new Timestamp(1704067200000L);
        final List<Object> params = Arrays.asList(ts);

        GreptimeDBQueryHelper.setParameters(ps, params);

        verify(ps).setTimestamp(1, ts);
    }

    @Test
    void setParametersShouldHandleMixedTypes() throws SQLException {
        final PreparedStatement ps = Mockito.mock(PreparedStatement.class);
        final Timestamp ts = new Timestamp(1704067200000L);
        final List<Object> params = Arrays.asList("hello", 42L, 7, ts);

        GreptimeDBQueryHelper.setParameters(ps, params);

        verify(ps).setString(1, "hello");
        verify(ps).setLong(2, 42L);
        verify(ps).setInt(3, 7);
        verify(ps).setTimestamp(4, ts);
    }

    // ---- buildJsonPathMatchExpr ----

    @Test
    void buildJsonPathMatchExprShouldBuildCorrectExpression() {
        assertEquals(
            "$[\"http.method\"] == \"GET\"",
            GreptimeDBQueryHelper.buildJsonPathMatchExpr("http.method", "GET")
        );
    }

    @Test
    void buildJsonPathMatchExprShouldEscapeQuotes() {
        assertEquals(
            "$[\"k\\\"ey\"] == \"v\\\"al\"",
            GreptimeDBQueryHelper.buildJsonPathMatchExpr("k\"ey", "v\"al")
        );
    }

    @Test
    void buildJsonPathMatchExprShouldEscapeBackslashes() {
        assertEquals(
            "$[\"k\\\\ey\"] == \"v\\\\al\"",
            GreptimeDBQueryHelper.buildJsonPathMatchExpr("k\\ey", "v\\al")
        );
    }
}
