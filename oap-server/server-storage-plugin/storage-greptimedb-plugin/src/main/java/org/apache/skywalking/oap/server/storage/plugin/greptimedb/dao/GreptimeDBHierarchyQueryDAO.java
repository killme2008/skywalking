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
import java.util.List;
import java.util.Map;
import org.apache.skywalking.oap.server.core.analysis.Layer;
import org.apache.skywalking.oap.server.core.hierarchy.instance.InstanceHierarchyRelationTraffic;
import org.apache.skywalking.oap.server.core.hierarchy.service.ServiceHierarchyRelationTraffic;
import org.apache.skywalking.oap.server.core.storage.query.IHierarchyQueryDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBConverter;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBStorageClient;

public class GreptimeDBHierarchyQueryDAO implements IHierarchyQueryDAO {
    private final GreptimeDBStorageClient client;
    private final int queryMaxSize;

    public GreptimeDBHierarchyQueryDAO(final GreptimeDBStorageClient client,
                                       final int metadataQueryMaxSize) {
        this.client = client;
        this.queryMaxSize = metadataQueryMaxSize * 2;
    }

    @Override
    public List<ServiceHierarchyRelationTraffic> readAllServiceHierarchyRelations() throws Exception {
        final String sql = "select * from " + ServiceHierarchyRelationTraffic.INDEX_NAME
            + " limit " + queryMaxSize;
        final List<ServiceHierarchyRelationTraffic> results = new ArrayList<>();
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                final Map<String, Object> map = GreptimeDBConverter.resultSetToGenericMap(rs);
                results.add(new ServiceHierarchyRelationTraffic.Builder()
                    .storage2Entity(new GreptimeDBConverter.ToEntity(map)));
            }
        } catch (SQLException e) {
            throw new IOException("Failed to read service hierarchy relations", e);
        }
        return results;
    }

    @Override
    public List<InstanceHierarchyRelationTraffic> readInstanceHierarchyRelations(
            final String instanceId, final String layer) throws Exception {
        final int layerValue = Layer.valueOf(layer).value();
        final String sql = "select * from " + InstanceHierarchyRelationTraffic.INDEX_NAME
            + " where ((" + InstanceHierarchyRelationTraffic.INSTANCE_ID + " = ?"
            + " and " + InstanceHierarchyRelationTraffic.SERVICE_LAYER + " = ?)"
            + " or (" + InstanceHierarchyRelationTraffic.RELATED_INSTANCE_ID + " = ?"
            + " and " + InstanceHierarchyRelationTraffic.RELATED_SERVICE_LAYER + " = ?))";

        final List<InstanceHierarchyRelationTraffic> results = new ArrayList<>();
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, instanceId);
            ps.setInt(2, layerValue);
            ps.setString(3, instanceId);
            ps.setInt(4, layerValue);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    final Map<String, Object> map = GreptimeDBConverter.resultSetToGenericMap(rs);
                    results.add(new InstanceHierarchyRelationTraffic.Builder()
                        .storage2Entity(new GreptimeDBConverter.ToEntity(map)));
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to read instance hierarchy relations", e);
        }
        return results;
    }
}
