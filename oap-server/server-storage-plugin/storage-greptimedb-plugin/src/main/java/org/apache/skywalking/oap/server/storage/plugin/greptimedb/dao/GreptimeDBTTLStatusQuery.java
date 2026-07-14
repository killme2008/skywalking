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

import java.math.BigInteger;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.core.storage.ttl.MetricsTTL;
import org.apache.skywalking.oap.server.core.storage.ttl.RecordsTTL;
import org.apache.skywalking.oap.server.core.storage.ttl.StorageTTLStatusQuery;
import org.apache.skywalking.oap.server.core.storage.ttl.TTLDefinition;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBStorageConfig;

/**
 * Reports the TTL configured on GreptimeDB tables.
 */
public class GreptimeDBTTLStatusQuery implements StorageTTLStatusQuery {
    private static final Pattern DURATION_PART_PATTERN = Pattern.compile("\\G\\s*(\\d+)\\s*([A-Za-z]+)");
    private static final BigInteger NANOS_PER_DAY = BigInteger.valueOf(86_400_000_000_000L);

    private final int metricsTTLDays;
    private final int recordsTTLDays;

    public GreptimeDBTTLStatusQuery(final GreptimeDBStorageConfig config) {
        metricsTTLDays = toDays(config.getMetricsTTL(), "metricsTTL");
        recordsTTLDays = toDays(config.getRecordsTTL(), "recordsTTL");
    }

    @Override
    public TTLDefinition getTTL() {
        return new TTLDefinition(
            new MetricsTTL(metricsTTLDays, metricsTTLDays, metricsTTLDays, metricsTTLDays),
            new RecordsTTL(
                recordsTTLDays,
                recordsTTLDays,
                recordsTTLDays,
                recordsTTLDays,
                recordsTTLDays
            )
        );
    }

    @Override
    public int getMetricsTTL(final Model model) {
        return metricsTTLDays;
    }

    static int toDays(final String duration, final String configName) {
        final String normalized = duration == null ? "" : duration.trim();
        if (normalized.isEmpty()
            || "forever".equalsIgnoreCase(normalized)
            || "null".equalsIgnoreCase(normalized)) {
            return Integer.MAX_VALUE;
        }
        if ("instant".equalsIgnoreCase(normalized)) {
            return 0;
        }

        final Matcher matcher = DURATION_PART_PATTERN.matcher(normalized);
        BigInteger nanos = BigInteger.ZERO;
        int parsedUntil = 0;
        while (matcher.find()) {
            final BigInteger value = new BigInteger(matcher.group(1));
            nanos = nanos.add(value.multiply(nanosPerUnit(matcher.group(2), configName, duration)));
            parsedUntil = matcher.end();
        }
        if (parsedUntil != normalized.length()) {
            throw new IllegalArgumentException(
                configName + " must be 'forever', 'instant', or a GreptimeDB duration: " + duration);
        }
        if (nanos.signum() == 0) {
            return Integer.MAX_VALUE;
        }

        final BigInteger[] quotientAndRemainder = nanos.divideAndRemainder(NANOS_PER_DAY);
        final BigInteger roundedUpDays = quotientAndRemainder[1].signum() == 0
            ? quotientAndRemainder[0] : quotientAndRemainder[0].add(BigInteger.ONE);
        return roundedUpDays.compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) > 0
            ? Integer.MAX_VALUE : roundedUpDays.intValue();
    }

    private static BigInteger nanosPerUnit(final String rawUnit,
                                           final String configName,
                                           final String duration) {
        if ("M".equals(rawUnit)) {
            return BigInteger.valueOf(2_630_016_000_000_000L);
        }
        switch (rawUnit.toLowerCase(Locale.ROOT)) {
            case "nsec":
            case "ns":
                return BigInteger.ONE;
            case "usec":
            case "us":
                return BigInteger.valueOf(1_000L);
            case "msec":
            case "ms":
                return BigInteger.valueOf(1_000_000L);
            case "seconds":
            case "second":
            case "sec":
            case "s":
                return BigInteger.valueOf(1_000_000_000L);
            case "minutes":
            case "minute":
            case "min":
            case "m":
                return BigInteger.valueOf(60_000_000_000L);
            case "hours":
            case "hour":
            case "hr":
            case "h":
                return BigInteger.valueOf(3_600_000_000_000L);
            case "days":
            case "day":
            case "d":
                return NANOS_PER_DAY;
            case "weeks":
            case "week":
            case "w":
                return NANOS_PER_DAY.multiply(BigInteger.valueOf(7L));
            case "months":
            case "month":
                return BigInteger.valueOf(2_630_016_000_000_000L);
            case "years":
            case "year":
            case "y":
                return BigInteger.valueOf(31_557_600_000_000_000L);
            default:
                throw new IllegalArgumentException(
                    configName + " contains an unsupported duration unit in: " + duration);
        }
    }
}
