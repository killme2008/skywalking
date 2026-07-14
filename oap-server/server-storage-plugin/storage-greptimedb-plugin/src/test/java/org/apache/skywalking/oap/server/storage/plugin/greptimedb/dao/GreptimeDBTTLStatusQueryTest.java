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

import org.apache.skywalking.oap.server.core.storage.ttl.TTLDefinition;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBStorageConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GreptimeDBTTLStatusQueryTest {

    @Test
    void shouldReportConfiguredTTL() {
        final GreptimeDBStorageConfig config = new GreptimeDBStorageConfig();
        config.setMetricsTTL("720h");
        config.setRecordsTTL("5d");

        final GreptimeDBTTLStatusQuery query = new GreptimeDBTTLStatusQuery(config);
        final TTLDefinition ttl = query.getTTL();

        assertEquals(30, ttl.getMetrics().getMetadata());
        assertEquals(30, ttl.getMetrics().getMinute());
        assertEquals(30, ttl.getMetrics().getHour());
        assertEquals(30, ttl.getMetrics().getDay());
        assertEquals(5, ttl.getRecords().getNormal());
        assertEquals(5, ttl.getRecords().getTrace());
        assertEquals(5, ttl.getRecords().getZipkinTrace());
        assertEquals(5, ttl.getRecords().getLog());
        assertEquals(5, ttl.getRecords().getBrowserErrorLog());
        assertEquals(30, query.getMetricsTTL(null));
    }

    @Test
    void shouldRoundSubDayTTLUpForDayBasedCoreChecks() {
        assertEquals(2, GreptimeDBTTLStatusQuery.toDays("25h", "metricsTTL"));
        assertEquals(1, GreptimeDBTTLStatusQuery.toDays("1m", "metricsTTL"));
        assertEquals(1, GreptimeDBTTLStatusQuery.toDays("1h 30m 10s", "metricsTTL"));
        assertEquals(31, GreptimeDBTTLStatusQuery.toDays("1M", "metricsTTL"));
        assertEquals(8, GreptimeDBTTLStatusQuery.toDays("1week 1day", "metricsTTL"));
        assertEquals(Integer.MAX_VALUE, GreptimeDBTTLStatusQuery.toDays("0d", "metricsTTL"));
        assertEquals(Integer.MAX_VALUE, GreptimeDBTTLStatusQuery.toDays("NULL", "metricsTTL"));
        assertEquals(0, GreptimeDBTTLStatusQuery.toDays("instant", "metricsTTL"));
    }

    @Test
    void shouldRejectUnsupportedTTL() {
        assertThrows(
            IllegalArgumentException.class,
            () -> GreptimeDBTTLStatusQuery.toDays("one week", "metricsTTL")
        );
    }
}
