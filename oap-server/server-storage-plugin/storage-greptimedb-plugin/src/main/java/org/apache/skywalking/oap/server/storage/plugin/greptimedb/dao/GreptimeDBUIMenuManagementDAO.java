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
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.oap.server.core.management.ui.menu.UIMenu;
import org.apache.skywalking.oap.server.core.storage.management.UIMenuManagementDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBConverter;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBStorageClient;

@Slf4j
@RequiredArgsConstructor
public class GreptimeDBUIMenuManagementDAO implements UIMenuManagementDAO {
    private final GreptimeDBStorageClient client;

    @Override
    public UIMenu getMenu(final String id) throws IOException {
        final String sql = "select * from " + UIMenu.INDEX_NAME
            + " where " + UIMenu.MENU_ID + " = ? limit 1";
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    final Map<String, Object> map = GreptimeDBConverter.resultSetToGenericMap(rs);
                    return new UIMenu.Builder()
                        .storage2Entity(new GreptimeDBConverter.ToEntity(map));
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to get menu: " + id, e);
        }
        return null;
    }

    @Override
    public void saveMenu(final UIMenu menu) throws IOException {
        final String sql = "insert into " + UIMenu.INDEX_NAME
            + " (" + UIMenu.MENU_ID
            + ", " + UIMenu.CONFIGURATION
            + ", " + UIMenu.UPDATE_TIME + ") values (?, ?, ?)";
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, menu.getMenuId());
            ps.setString(2, menu.getConfigurationJson());
            ps.setLong(3, menu.getUpdateTime());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IOException("Failed to save menu: " + menu.getMenuId(), e);
        }
    }
}
