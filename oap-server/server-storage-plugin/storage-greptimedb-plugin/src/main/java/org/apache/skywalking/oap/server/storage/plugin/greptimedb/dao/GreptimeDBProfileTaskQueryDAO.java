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
import org.apache.skywalking.oap.server.core.profiling.trace.ProfileTaskRecord;
import org.apache.skywalking.oap.server.core.query.type.ProfileTask;
import org.apache.skywalking.oap.server.core.storage.profiling.trace.IProfileTaskQueryDAO;
import org.apache.skywalking.oap.server.library.util.StringUtil;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBStorageClient;

import static org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBQueryHelper.addTimestampConditions;

@RequiredArgsConstructor
public class GreptimeDBProfileTaskQueryDAO implements IProfileTaskQueryDAO {
    private final GreptimeDBStorageClient client;

    @Override
    public List<ProfileTask> getTaskList(final String serviceId, final String endpointName,
                                         final Long startTimeBucket, final Long endTimeBucket,
                                         final Integer limit) throws IOException {
        final StringBuilder sql = new StringBuilder();
        final List<Object> params = new ArrayList<>();
        sql.append("select * from ").append(ProfileTaskRecord.INDEX_NAME);

        final List<String> conditions = new ArrayList<>();
        if (StringUtil.isNotEmpty(serviceId)) {
            conditions.add(ProfileTaskRecord.SERVICE_ID + " = ?");
            params.add(serviceId);
        }
        if (StringUtil.isNotEmpty(endpointName)) {
            conditions.add(ProfileTaskRecord.ENDPOINT_NAME + " = ?");
            params.add(endpointName);
        }
        addTimestampConditions(conditions, params, startTimeBucket, endTimeBucket);

        if (!conditions.isEmpty()) {
            sql.append(" where ");
            sql.append(String.join(" and ", conditions));
        }
        sql.append(" order by ").append(ProfileTaskRecord.START_TIME).append(" desc");
        if (limit != null) {
            sql.append(" limit ").append(limit);
        }

        final List<ProfileTask> tasks = new ArrayList<>();
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) {
                ps.setObject(i + 1, params.get(i));
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tasks.add(buildProfileTask(rs));
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to query profile tasks", e);
        }
        return tasks;
    }

    @Override
    public ProfileTask getById(final String id) throws IOException {
        final String sql = "select * from " + ProfileTaskRecord.INDEX_NAME
            + " where " + ProfileTaskRecord.TASK_ID + " = ? limit 1";
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return buildProfileTask(rs);
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to get profile task: " + id, e);
        }
        return null;
    }

    private ProfileTask buildProfileTask(final ResultSet rs) throws SQLException {
        return ProfileTask.builder()
            .id(rs.getString(ProfileTaskRecord.TASK_ID))
            .serviceId(rs.getString(ProfileTaskRecord.SERVICE_ID))
            .endpointName(rs.getString(ProfileTaskRecord.ENDPOINT_NAME))
            .startTime(rs.getLong(ProfileTaskRecord.START_TIME))
            .createTime(rs.getLong(ProfileTaskRecord.CREATE_TIME))
            .duration(rs.getInt(ProfileTaskRecord.DURATION))
            .minDurationThreshold(rs.getInt(ProfileTaskRecord.MIN_DURATION_THRESHOLD))
            .dumpPeriod(rs.getInt(ProfileTaskRecord.DUMP_PERIOD))
            .maxSamplingCount(rs.getInt(ProfileTaskRecord.MAX_SAMPLING_COUNT))
            .build();
    }
}
