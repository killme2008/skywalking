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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.skywalking.oap.server.core.analysis.metrics.DataTable;
import org.apache.skywalking.oap.server.core.analysis.metrics.Metrics;
import org.apache.skywalking.oap.server.core.query.PointOfTime;
import org.apache.skywalking.oap.server.core.query.input.Duration;
import org.apache.skywalking.oap.server.core.query.input.MetricsCondition;
import org.apache.skywalking.oap.server.core.query.type.HeatMap;
import org.apache.skywalking.oap.server.core.query.type.KVInt;
import org.apache.skywalking.oap.server.core.query.type.KeyValue;
import org.apache.skywalking.oap.server.core.query.type.MetricsValues;
import org.apache.skywalking.oap.server.core.storage.annotation.ValueColumnMetadata;
import org.apache.skywalking.oap.server.core.storage.query.IMetricsQueryDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBConverter;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBStorageClient;

@RequiredArgsConstructor
public class GreptimeDBMetricsQueryDAO implements IMetricsQueryDAO {
    private final GreptimeDBStorageClient client;

    @Override
    public MetricsValues readMetricsValues(final MetricsCondition condition,
                                            final String valueColumnName,
                                            final Duration duration) throws IOException {
        final MetricsValues metricsValues = new MetricsValues();
        final var intValues = metricsValues.getValues();
        final String tableName = GreptimeDBConverter.resolveMetricsTableName(
            condition.getName(), duration.getStep());

        final List<PointOfTime> pointOfTimes = duration.assembleDurationPoints();
        final String entityId = condition.getEntity().buildId();
        final List<String> ids = pointOfTimes.stream()
            .map(pointOfTime -> pointOfTime.id(entityId))
            .collect(Collectors.toList());

        if (ids.isEmpty()) {
            return metricsValues;
        }

        final String placeholders = ids.stream().map(id -> "?").collect(Collectors.joining(","));
        final String sql = "select `id`, " + valueColumnName + " from " + tableName
            + " where `id` in (" + placeholders + ")";

        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < ids.size(); i++) {
                ps.setString(i + 1, ids.get(i));
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    final KVInt kv = new KVInt();
                    kv.setId(rs.getString("id"));
                    kv.setValue(rs.getLong(valueColumnName));
                    intValues.addKVInt(kv);
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to read metrics values from " + tableName, e);
        }

        metricsValues.setValues(
            Util.sortValues(intValues, ids,
                ValueColumnMetadata.INSTANCE.getDefaultValue(condition.getName()))
        );
        return metricsValues;
    }

    @Override
    public List<MetricsValues> readLabeledMetricsValues(final MetricsCondition condition,
                                                         final String valueColumnName,
                                                         final List<KeyValue> labels,
                                                         final Duration duration) throws IOException {
        final String tableName = GreptimeDBConverter.resolveMetricsTableName(
            condition.getName(), duration.getStep());
        final Map<String, DataTable> idMap = new HashMap<>();

        final List<PointOfTime> pointOfTimes = duration.assembleDurationPoints();
        final String entityId = condition.getEntity().buildId();
        final List<String> ids = pointOfTimes.stream()
            .map(pointOfTime -> pointOfTime.id(entityId))
            .collect(Collectors.toList());

        if (ids.isEmpty()) {
            return new ArrayList<>();
        }

        final String placeholders = ids.stream().map(id -> "?").collect(Collectors.joining(","));
        final String sql = "select `id`, " + valueColumnName + " from " + tableName
            + " where `id` in (" + placeholders + ")";

        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < ids.size(); i++) {
                ps.setString(i + 1, ids.get(i));
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    final String id = rs.getString("id");
                    final DataTable multipleValues = new DataTable(5);
                    multipleValues.toObject(rs.getString(valueColumnName));
                    idMap.put(id, multipleValues);
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to read labeled metrics values from " + tableName, e);
        }

        return Util.sortValues(
            Util.composeLabelValue(condition.getName(), labels, ids, idMap),
            ids,
            ValueColumnMetadata.INSTANCE.getDefaultValue(condition.getName())
        );
    }

    @Override
    public List<MetricsValues> readLabeledMetricsValuesWithoutEntity(final String metricName,
                                                                      final String valueColumnName,
                                                                      final List<KeyValue> labels,
                                                                      final Duration duration) throws IOException {
        final String tableName = GreptimeDBConverter.resolveMetricsTableName(
            metricName, duration.getStep());
        final Map<String, DataTable> idMap = new HashMap<>();

        final String sql = "select `id`, " + valueColumnName + " from " + tableName
            + " where " + Metrics.TIME_BUCKET + " >= ? and " + Metrics.TIME_BUCKET + " <= ?"
            + " limit " + METRICS_VALUES_WITHOUT_ENTITY_LIMIT;

        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, duration.getStartTimeBucket());
            ps.setLong(2, duration.getEndTimeBucket());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    final String id = rs.getString("id");
                    final DataTable multipleValues = new DataTable(rs.getString(valueColumnName));
                    idMap.put(id, multipleValues);
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to read labeled metrics without entity from " + tableName, e);
        }

        final Map<String, DataTable> result = idMap.entrySet().stream()
            .limit(METRICS_VALUES_WITHOUT_ENTITY_LIMIT)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        final List<String> ids = new ArrayList<>(result.keySet());
        return Util.sortValues(
            Util.composeLabelValue(metricName, labels, ids, result),
            ids,
            ValueColumnMetadata.INSTANCE.getDefaultValue(metricName)
        );
    }

    @Override
    public HeatMap readHeatMap(final MetricsCondition condition,
                                final String valueColumnName,
                                final Duration duration) throws IOException {
        final String tableName = GreptimeDBConverter.resolveMetricsTableName(
            condition.getName(), duration.getStep());
        final HeatMap heatMap = new HeatMap();

        final List<PointOfTime> pointOfTimes = duration.assembleDurationPoints();
        final String entityId = condition.getEntity().buildId();
        final List<String> ids = pointOfTimes.stream()
            .map(pointOfTime -> pointOfTime.id(entityId))
            .collect(Collectors.toList());

        if (ids.isEmpty()) {
            return heatMap;
        }

        final String placeholders = ids.stream().map(id -> "?").collect(Collectors.joining(","));
        final String sql = "select `id`, " + valueColumnName + " as dataset from " + tableName
            + " where `id` in (" + placeholders + ")";
        final int defaultValue = ValueColumnMetadata.INSTANCE.getDefaultValue(condition.getName());

        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < ids.size(); i++) {
                ps.setString(i + 1, ids.get(i));
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    heatMap.buildColumn(rs.getString("id"), rs.getString("dataset"), defaultValue);
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to read heat map from " + tableName, e);
        }

        heatMap.fixMissingColumns(ids, defaultValue);
        return heatMap;
    }
}
