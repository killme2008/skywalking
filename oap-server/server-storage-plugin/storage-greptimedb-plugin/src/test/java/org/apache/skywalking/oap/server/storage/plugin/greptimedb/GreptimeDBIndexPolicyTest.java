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
 */

package org.apache.skywalking.oap.server.storage.plugin.greptimedb;

import java.util.Collections;
import org.apache.skywalking.oap.server.core.analysis.DownSampling;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBIndexPolicy.IndexType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GreptimeDBIndexPolicyTest {
    private final GreptimeDBIndexPolicy policy = new GreptimeDBIndexPolicy();

    @Test
    void shouldClassifyIndexesByConcreteQueryPredicate() {
        assertEquals(IndexType.SKIPPING, resolve("profile_task", "task_id"));
        assertEquals(IndexType.SKIPPING, resolve("span_attached_event_record", "related_trace_id"));
        assertEquals(IndexType.INVERTED, resolve("zipkin_span", "local_endpoint_service_name"));
        assertEquals(IndexType.FULLTEXT, resolve("log", "content"));
    }

    @Test
    void shouldLeaveRangeAndUnknownColumnsWithoutSecondaryIndex() {
        assertEquals(IndexType.NONE, resolve("zipkin_span", "duration"));
        assertEquals(IndexType.NONE, resolve("segment", "latency"));
        assertEquals(IndexType.NONE, resolve("custom_metric", "custom_value"));
    }

    private IndexType resolve(final String modelName, final String columnName) {
        final Model model = TestModels.metricsModel(
            modelName, DownSampling.Minute,
            Collections.singletonList(TestModels.col(columnName, String.class)));
        return policy.resolve(model, model.getColumns().get(0));
    }
}
