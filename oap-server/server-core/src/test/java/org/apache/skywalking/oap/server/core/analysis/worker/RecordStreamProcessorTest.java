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

package org.apache.skywalking.oap.server.core.analysis.worker;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.skywalking.oap.server.core.analysis.DownSampling;
import org.apache.skywalking.oap.server.core.analysis.TimeBucket;
import org.apache.skywalking.oap.server.core.analysis.record.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class RecordStreamProcessorTest {
    private RecordStreamProcessor processor;
    private RecordPersistentWorker worker;
    private Record record;
    private long expirationTime;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception {
        processor = new RecordStreamProcessor();
        processor.setRecordDataTTL(3);
        setTestingTTL(false);
        worker = mock(RecordPersistentWorker.class);
        record = mock(Record.class);
        final long timeBucket = 202601010000L;
        when(record.getTimeBucket()).thenReturn(timeBucket);
        expirationTime = TimeBucket.getTimestamp(timeBucket, DownSampling.Minute) + TimeUnit.DAYS.toMillis(3);

        final Field workersField = RecordStreamProcessor.class.getDeclaredField("workers");
        workersField.setAccessible(true);
        final Map<Class<? extends Record>, RecordPersistentWorker> workers =
            (Map<Class<? extends Record>, RecordPersistentWorker>) workersField.get(processor);
        workers.put(record.getClass(), worker);
    }

    @ParameterizedTest
    @ValueSource(longs = {-1, 0})
    void acceptsRecordsWithinTTL(long offset) {
        processor.in(record, expirationTime + offset);

        verify(worker).in(record);
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 60000})
    void filtersRecordsBeyondTTL(long offset) {
        processor.in(record, expirationTime + offset);

        verifyNoInteractions(worker);
    }

    @Test
    void testingTTLBypassesExpiration() throws Exception {
        setTestingTTL(true);

        processor.in(record, expirationTime + 1);

        verify(worker).in(record);
    }

    private void setTestingTTL(boolean enabled) throws Exception {
        final Field field = RecordStreamProcessor.class.getDeclaredField("isTestingTTL");
        field.setAccessible(true);
        field.setBoolean(processor, enabled);
    }
}
