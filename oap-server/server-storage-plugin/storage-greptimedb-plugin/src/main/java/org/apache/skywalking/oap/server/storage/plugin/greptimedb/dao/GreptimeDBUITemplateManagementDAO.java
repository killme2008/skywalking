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
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.oap.server.core.management.ui.template.UITemplate;
import org.apache.skywalking.oap.server.core.query.input.DashboardSetting;
import org.apache.skywalking.oap.server.core.query.type.DashboardConfiguration;
import org.apache.skywalking.oap.server.core.query.type.TemplateChangeStatus;
import org.apache.skywalking.oap.server.core.storage.management.UITemplateManagementDAO;
import org.apache.skywalking.oap.server.library.util.BooleanUtils;
import org.apache.skywalking.oap.server.library.util.StringUtil;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBConverter;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBStorageClient;

@Slf4j
@RequiredArgsConstructor
public class GreptimeDBUITemplateManagementDAO implements UITemplateManagementDAO {
    private final GreptimeDBStorageClient client;

    @Override
    public DashboardConfiguration getTemplate(final String id) throws IOException {
        if (StringUtil.isEmpty(id)) {
            return null;
        }
        final String sql = "select * from " + UITemplate.INDEX_NAME
            + " where " + UITemplate.TEMPLATE_ID + " = ? limit 1";
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    final Map<String, Object> map = GreptimeDBConverter.resultSetToGenericMap(rs);
                    final UITemplate uiTemplate = new UITemplate.Builder()
                        .storage2Entity(new GreptimeDBConverter.ToEntity(map));
                    return new DashboardConfiguration().fromEntity(uiTemplate);
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to get template: " + id, e);
        }
        return null;
    }

    @Override
    public List<DashboardConfiguration> getAllTemplates(final Boolean includingDisabled) throws IOException {
        final StringBuilder sql = new StringBuilder();
        final List<Object> params = new ArrayList<>();
        sql.append("select * from ").append(UITemplate.INDEX_NAME);
        if (!includingDisabled) {
            sql.append(" where ").append(UITemplate.DISABLED).append(" = ?");
            params.add(BooleanUtils.booleanToValue(false));
        }
        final List<DashboardConfiguration> configs = new ArrayList<>();
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) {
                ps.setObject(i + 1, params.get(i));
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    final Map<String, Object> map = GreptimeDBConverter.resultSetToGenericMap(rs);
                    final UITemplate uiTemplate = new UITemplate.Builder()
                        .storage2Entity(new GreptimeDBConverter.ToEntity(map));
                    configs.add(new DashboardConfiguration().fromEntity(uiTemplate));
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to get all templates", e);
        }
        return configs;
    }

    @Override
    public TemplateChangeStatus addTemplate(final DashboardSetting setting) throws IOException {
        final UITemplate uiTemplate = setting.toEntity();
        return insertOrUpdate(uiTemplate);
    }

    @Override
    public TemplateChangeStatus changeTemplate(final DashboardSetting setting) throws IOException {
        final UITemplate uiTemplate = setting.toEntity();
        return insertOrUpdate(uiTemplate);
    }

    @Override
    public TemplateChangeStatus disableTemplate(final String id) throws IOException {
        final String sql = "insert into " + UITemplate.INDEX_NAME
            + " (" + UITemplate.TEMPLATE_ID
            + ", " + UITemplate.CONFIGURATION
            + ", " + UITemplate.UPDATE_TIME
            + ", " + UITemplate.DISABLED + ") values (?, ?, ?, ?)";
        // First, get the existing template to preserve configuration
        final DashboardConfiguration existing = getTemplate(id);
        if (existing == null) {
            return TemplateChangeStatus.builder().status(false).id(id)
                .message("Can't find the template").build();
        }
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, id);
            ps.setString(2, existing.getConfiguration());
            ps.setLong(3, System.currentTimeMillis());
            ps.setInt(4, BooleanUtils.TRUE);
            ps.executeUpdate();
            return TemplateChangeStatus.builder().status(true).id(id).build();
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            return TemplateChangeStatus.builder().status(false).id(id)
                .message("Can't disable the template").build();
        }
    }

    private TemplateChangeStatus insertOrUpdate(final UITemplate uiTemplate) {
        final String sql = "insert into " + UITemplate.INDEX_NAME
            + " (" + UITemplate.TEMPLATE_ID
            + ", " + UITemplate.CONFIGURATION
            + ", " + UITemplate.UPDATE_TIME
            + ", " + UITemplate.DISABLED + ") values (?, ?, ?, ?)";
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, uiTemplate.getTemplateId());
            ps.setString(2, uiTemplate.getConfiguration());
            ps.setLong(3, uiTemplate.getUpdateTime());
            ps.setInt(4, uiTemplate.getDisabled());
            ps.executeUpdate();
            return TemplateChangeStatus.builder().status(true).id(uiTemplate.getTemplateId()).build();
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            return TemplateChangeStatus.builder().status(false).id(uiTemplate.getTemplateId())
                .message("Can't add/update the template").build();
        }
    }
}
