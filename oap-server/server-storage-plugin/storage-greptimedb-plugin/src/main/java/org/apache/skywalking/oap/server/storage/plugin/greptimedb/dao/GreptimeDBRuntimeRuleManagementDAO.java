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
import lombok.RequiredArgsConstructor;
import org.apache.skywalking.oap.server.core.management.runtimerule.RuntimeRule;
import org.apache.skywalking.oap.server.core.storage.management.RuntimeRuleManagementDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBConverter;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBStorageClient;

/**
 * Runtime-rule read/upsert/delete for GreptimeDB. Like the other management tables, upsert semantics
 * come from an explicit synthetic {@code id} plus a constant {@code greptime_ts} (merge_mode=last_row),
 * so {@code save} overwrites in place instead of appending a version.
 */
@RequiredArgsConstructor
public class GreptimeDBRuntimeRuleManagementDAO implements RuntimeRuleManagementDAO {
    private final GreptimeDBStorageClient client;

    @Override
    public List<RuntimeRuleFile> getAll() throws IOException {
        final String sql = "select " + RuntimeRule.CATALOG + ", " + RuntimeRule.NAME + ", "
            + RuntimeRule.CONTENT + ", " + RuntimeRule.STATUS + ", " + RuntimeRule.UPDATE_TIME
            + " from " + RuntimeRule.INDEX_NAME;
        final List<RuntimeRuleFile> files = new ArrayList<>();
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                files.add(new RuntimeRuleFile(
                    rs.getString(RuntimeRule.CATALOG),
                    rs.getString(RuntimeRule.NAME),
                    rs.getString(RuntimeRule.CONTENT),
                    rs.getString(RuntimeRule.STATUS),
                    rs.getLong(RuntimeRule.UPDATE_TIME)));
            }
        } catch (SQLException e) {
            throw new IOException("Failed to read runtime rules", e);
        }
        return files;
    }

    @Override
    public void save(final RuntimeRule rule) throws IOException {
        // Explicit id (= entity.id().build()) + constant greptime_ts make merge_mode=last_row overwrite
        // the prior row rather than append a new version.
        final String sql = "insert into " + RuntimeRule.INDEX_NAME
            + " (" + GreptimeDBConverter.quoteColumn("id")
            + ", " + RuntimeRule.CATALOG
            + ", " + RuntimeRule.NAME
            + ", " + RuntimeRule.CONTENT
            + ", " + RuntimeRule.STATUS
            + ", " + RuntimeRule.UPDATE_TIME
            + ", " + GreptimeDBConverter.quoteColumn("greptime_ts") + ") values (?, ?, ?, ?, ?, ?, ?)";
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, rule.id().build());
            ps.setString(2, rule.getCatalog());
            ps.setString(3, rule.getName());
            ps.setString(4, rule.getContent());
            ps.setString(5, rule.getStatus());
            ps.setLong(6, rule.getUpdateTime());
            ps.setLong(7, GreptimeDBConverter.MANAGEMENT_TIMESTAMP);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IOException("Failed to save runtime rule "
                + rule.getCatalog() + ":" + rule.getName(), e);
        }
    }

    @Override
    public void delete(final String catalog, final String name) throws IOException {
        // The synthetic id is a deterministic function of (catalog, name); delete by it (the PRIMARY KEY).
        final RuntimeRule probe = new RuntimeRule();
        probe.setCatalog(catalog);
        probe.setName(name);
        final String sql = "delete from " + RuntimeRule.INDEX_NAME
            + " where " + GreptimeDBConverter.quoteColumn("id") + " = ?";
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, probe.id().build());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IOException("Failed to delete runtime rule " + catalog + ":" + name, e);
        }
    }
}
