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

import java.util.ArrayList;
import java.util.List;
import org.apache.skywalking.oap.server.core.analysis.DownSampling;
import org.apache.skywalking.oap.server.core.analysis.config.NoneStream;
import org.apache.skywalking.oap.server.core.analysis.management.ManagementData;
import org.apache.skywalking.oap.server.core.analysis.metrics.Metrics;
import org.apache.skywalking.oap.server.core.analysis.record.Record;
import org.apache.skywalking.oap.server.core.storage.annotation.Column;
import org.apache.skywalking.oap.server.core.storage.model.BanyanDBExtension;
import org.apache.skywalking.oap.server.core.storage.model.BanyanDBModelExtension;
import org.apache.skywalking.oap.server.core.storage.model.ColumnName;
import org.apache.skywalking.oap.server.core.storage.model.ElasticSearchExtension;
import org.apache.skywalking.oap.server.core.storage.model.ElasticSearchModelExtension;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.core.storage.model.ModelColumn;
import org.apache.skywalking.oap.server.core.storage.model.SQLDatabaseExtension;
import org.apache.skywalking.oap.server.core.storage.model.SQLDatabaseModelExtension;

import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

/**
 * Helper for constructing Model and ModelColumn objects in tests.
 */
final class TestModels {

    private TestModels() {
    }

    static ModelColumn col(final String name, final Class<?> type,
                           final boolean storageOnly, final int length) {
        final Column annotation = mock(Column.class);
        lenient().when(annotation.name()).thenReturn(name);
        return new ModelColumn(
            new ColumnName(annotation),
            type, type,
            storageOnly, false, false,
            length,
            new SQLDatabaseExtension(),
            new ElasticSearchExtension(null, null, false, false, false),
            new BanyanDBExtension(-1, -1, !storageOnly, null, false, null, false)
        );
    }

    static ModelColumn col(final String name, final Class<?> type) {
        return col(name, type, false, 200);
    }

    static Model metricsModel(final String name, final DownSampling ds,
                               final List<ModelColumn> columns) {
        return new Model(
            name, columns, 1, ds,
            false, Metrics.class, true,
            new SQLDatabaseModelExtension(),
            new BanyanDBModelExtension(),
            new ElasticSearchModelExtension()
        );
    }

    static Model recordModel(final String name, final List<ModelColumn> columns) {
        return new Model(
            name, columns, 2, DownSampling.Second,
            false, Record.class, false,
            new SQLDatabaseModelExtension(),
            new BanyanDBModelExtension(),
            new ElasticSearchModelExtension()
        );
    }

    static Model managementModel(final String name, final List<ModelColumn> columns) {
        return new Model(
            name, columns, 3, DownSampling.None,
            false, ManagementData.class, false,
            new SQLDatabaseModelExtension(),
            new BanyanDBModelExtension(),
            new ElasticSearchModelExtension()
        );
    }

    static Model noneStreamModel(final String name, final List<ModelColumn> columns) {
        return new Model(
            name, columns, 4, DownSampling.None,
            false, NoneStream.class, false,
            new SQLDatabaseModelExtension(),
            new BanyanDBModelExtension(),
            new ElasticSearchModelExtension()
        );
    }

    /**
     * Build a typical metrics model resembling service_resp_time.
     */
    static Model sampleMetricsModel() {
        final List<ModelColumn> columns = new ArrayList<>();
        columns.add(col("service_id", String.class));
        columns.add(col("entity_id", String.class));
        columns.add(col("time_bucket", long.class));
        columns.add(col("summation", long.class, true, 0));
        columns.add(col("count", long.class, true, 0));
        columns.add(col("value", long.class, true, 0));
        return metricsModel("service_resp_time", DownSampling.Minute, columns);
    }

    /**
     * Build a typical record model resembling segment.
     */
    static Model sampleRecordModel() {
        final List<ModelColumn> columns = new ArrayList<>();
        columns.add(col("segment_id", String.class));
        columns.add(col("trace_id", String.class));
        columns.add(col("service_id", String.class));
        columns.add(col("service_instance_id", String.class));
        columns.add(col("endpoint_id", String.class));
        columns.add(col("start_time", long.class));
        columns.add(col("latency", int.class));
        columns.add(col("is_error", int.class));
        columns.add(col("tags", List.class));
        columns.add(col("data_binary", byte[].class, true, 0));
        columns.add(col("time_bucket", long.class));
        return recordModel("segment", columns);
    }

    /**
     * Build a typical management model resembling ui_template.
     */
    static Model sampleManagementModel() {
        final List<ModelColumn> columns = new ArrayList<>();
        columns.add(col("name", String.class));
        columns.add(col("type", String.class));
        columns.add(col("configuration", String.class, true, 0));
        columns.add(col("activated", int.class));
        columns.add(col("disabled", int.class));
        return managementModel("ui_template", columns);
    }
}
