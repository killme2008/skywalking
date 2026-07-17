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

package org.apache.skywalking.oap.server.storage.plugin.greptimedb;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.skywalking.oap.server.core.alarm.AlarmRecord;
import org.apache.skywalking.oap.server.core.analysis.manual.log.AbstractLogRecord;
import org.apache.skywalking.oap.server.core.analysis.manual.log.LogRecord;
import org.apache.skywalking.oap.server.core.analysis.manual.segment.SegmentRecord;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.core.storage.model.ModelColumn;
import org.apache.skywalking.oap.server.core.zipkin.ZipkinSpanRecord;

/** Indexes backed by concrete equality or text predicates in GreptimeDB query DAOs. */
public final class GreptimeDBIndexPolicy {
    private static final Map<String, IndexType> INDEXES;

    static {
        final Map<String, IndexType> indexes = new HashMap<>();
        skipping(indexes, SegmentRecord.INDEX_NAME,
            SegmentRecord.TRACE_ID, SegmentRecord.SEGMENT_ID,
            SegmentRecord.SERVICE_INSTANCE_ID, SegmentRecord.ENDPOINT_ID);
        skipping(indexes, LogRecord.INDEX_NAME,
            AbstractLogRecord.TRACE_ID, AbstractLogRecord.TRACE_SEGMENT_ID,
            AbstractLogRecord.SERVICE_INSTANCE_ID, AbstractLogRecord.ENDPOINT_ID);
        skipping(indexes, ZipkinSpanRecord.INDEX_NAME, ZipkinSpanRecord.TRACE_ID);
        inverted(indexes, ZipkinSpanRecord.INDEX_NAME,
            ZipkinSpanRecord.LOCAL_ENDPOINT_SERVICE_NAME,
            ZipkinSpanRecord.REMOTE_ENDPOINT_SERVICE_NAME,
            ZipkinSpanRecord.NAME);

        skipping(indexes, "alarm_record", "uuid", "id0", "id1");
        skipping(indexes, "alarm_recovery_record", "uuid");
        skipping(indexes, "profile_task", "task_id", "service_id");
        skipping(indexes, "profile_task_log", "task_id");
        skipping(indexes, "profile_task_segment_snapshot", "task_id", "segment_id");
        skipping(indexes, "pprof_task", "task_id", "service_id");
        skipping(indexes, "pprof_task_log", "task_id");
        skipping(indexes, "pprof_profiling_data", "task_id");
        skipping(indexes, "async_profiler_task", "task_id", "service_id");
        skipping(indexes, "async_profiler_task_log", "task_id");
        skipping(indexes, "jfr_profiling_data", "task_id");
        skipping(indexes, "ebpf_profiling_schedule", "task_id");
        skipping(indexes, "ebpf_profiling_task", "service_id", "logical_id");
        skipping(indexes, "ebpf_profiling_data", "task_id", "schedule_id");
        skipping(indexes, "span_attached_event_record", "related_trace_id");
        skipping(indexes, "sw_span_attached_event_record", "related_trace_id");

        skipping(indexes, "instance_traffic", "service_id");
        skipping(indexes, "endpoint_traffic", "service_id");
        skipping(indexes, "process_traffic", "service_id", "instance_id", "agent_id");
        skipping(indexes, "service_label", "service_id");
        skipping(indexes, "instance_hierarchy_relation", "instance_id", "related_instance_id");

        skipping(indexes, "event", "uuid", "service", "service_instance", "endpoint");
        inverted(indexes, "event", "name", "type", "layer");
        inverted(indexes, "tag_autocomplete", "tag_type", "tag_key");

        indexes.put(key(LogRecord.INDEX_NAME, AbstractLogRecord.CONTENT), IndexType.FULLTEXT);
        indexes.put(key(AlarmRecord.INDEX_NAME, AlarmRecord.SCOPE), IndexType.INVERTED);
        indexes.put(key(AlarmRecord.INDEX_NAME, AlarmRecord.LAYER), IndexType.INVERTED);
        indexes.put(key(AlarmRecord.INDEX_NAME, AlarmRecord.RULE_NAME), IndexType.INVERTED);
        INDEXES = Collections.unmodifiableMap(indexes);
    }

    public IndexType resolve(final Model model, final ModelColumn column) {
        return INDEXES.getOrDefault(
            key(model.getName(), column.getColumnName().getStorageName()), IndexType.NONE);
    }

    private static void skipping(final Map<String, IndexType> indexes,
                                 final String model, final String... columns) {
        for (final String column : columns) {
            indexes.put(key(model, column), IndexType.SKIPPING);
        }
    }

    private static void inverted(final Map<String, IndexType> indexes,
                                 final String model, final String... columns) {
        for (final String column : columns) {
            indexes.put(key(model, column), IndexType.INVERTED);
        }
    }

    private static String key(final String model, final String column) {
        return model + '\0' + column;
    }

    public enum IndexType {
        NONE(""),
        INVERTED(" INVERTED INDEX"),
        SKIPPING(" SKIPPING INDEX"),
        FULLTEXT(" FULLTEXT INDEX WITH(analyzer = 'English', case_sensitive = 'false')");

        private final String ddl;

        IndexType(final String ddl) {
            this.ddl = ddl;
        }

        public String ddl() {
            return ddl;
        }
    }
}
