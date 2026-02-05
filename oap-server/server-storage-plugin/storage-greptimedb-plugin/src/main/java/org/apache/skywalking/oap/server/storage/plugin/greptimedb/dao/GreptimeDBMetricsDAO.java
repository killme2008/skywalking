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

import io.greptime.models.Table;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.oap.server.core.analysis.metrics.Metrics;
import org.apache.skywalking.oap.server.core.storage.IMetricsDAO;
import org.apache.skywalking.oap.server.core.storage.SessionCacheCallback;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.core.storage.type.StorageBuilder;
import org.apache.skywalking.oap.server.library.client.request.InsertRequest;
import org.apache.skywalking.oap.server.library.client.request.UpdateRequest;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBConverter;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBStorageClient;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.SchemaRegistry;

@Slf4j
@RequiredArgsConstructor
public class GreptimeDBMetricsDAO implements IMetricsDAO {
    private final GreptimeDBStorageClient client;
    private final SchemaRegistry schemaRegistry;
    @SuppressWarnings("rawtypes")
    private final StorageBuilder storageBuilder;

    @Override
    @SuppressWarnings("unchecked")
    public List<Metrics> multiGet(final Model model, final List<Metrics> metrics) throws Exception {
        final String tableName = schemaRegistry.getTableName(model);
        final List<String> ids = metrics.stream()
            .map(m -> m.id().build())
            .collect(Collectors.toList());

        if (ids.isEmpty()) {
            return new ArrayList<>();
        }

        final String placeholders = ids.stream().map(id -> "?").collect(Collectors.joining(","));
        final String sql = "SELECT * FROM " + tableName + " WHERE `id` IN (" + placeholders + ")";

        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < ids.size(); i++) {
                ps.setString(i + 1, ids.get(i));
            }

            final List<Metrics> result = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    final Map<String, Object> rowMap = GreptimeDBConverter.resultSetToMap(rs, model);
                    final Metrics m = (Metrics) storageBuilder.storage2Entity(
                        new GreptimeDBConverter.ToEntity(rowMap));
                    result.add(m);
                }
            }
            return result;
        } catch (SQLException e) {
            throw new IOException("Failed to multiGet from " + tableName, e);
        }
    }

    @Override
    public InsertRequest prepareBatchInsert(final Model model, final Metrics metrics,
                                            final SessionCacheCallback callback) throws IOException {
        final Table table = buildMetricsTable(model, metrics);
        return new GreptimeDBInsertRequest(table, callback);
    }

    @Override
    public UpdateRequest prepareBatchUpdate(final Model model, final Metrics metrics,
                                            final SessionCacheCallback callback) throws IOException {
        // In GreptimeDB with merge_mode='last_row', update is an insert that overwrites
        final Table table = buildMetricsTable(model, metrics);
        return new GreptimeDBUpdateRequest(table, callback);
    }

    @SuppressWarnings("unchecked")
    private Table buildMetricsTable(final Model model, final Metrics metrics) {
        final SchemaRegistry.WriteSchemaInfo schemaInfo = schemaRegistry.getWriteSchema(model);
        final long greptimeTs = GreptimeDBConverter.timeBucketToTimestamp(
            metrics.getTimeBucket(), model.getDownsampling());
        return GreptimeDBTableBuilder.buildTable(metrics, storageBuilder, model, schemaInfo, greptimeTs);
    }
}
