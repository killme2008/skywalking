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

import com.google.gson.Gson;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.skywalking.oap.server.core.profiling.pprof.storage.PprofTaskRecord;
import org.apache.skywalking.oap.server.core.query.type.PprofEventType;
import org.apache.skywalking.oap.server.core.query.type.PprofTask;
import org.apache.skywalking.oap.server.core.storage.profiling.pprof.IPprofTaskQueryDAO;
import org.apache.skywalking.oap.server.library.util.StringUtil;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBStorageClient;

@RequiredArgsConstructor
public class GreptimeDBPprofTaskQueryDAO implements IPprofTaskQueryDAO {
    private static final Gson GSON = new Gson();
    private final GreptimeDBStorageClient client;

    @Override
    public List<PprofTask> getTaskList(final String serviceId,
                                        final Long startTimeBucket,
                                        final Long endTimeBucket,
                                        final Integer limit) throws IOException {
        final StringBuilder sql = new StringBuilder();
        final List<Object> params = new ArrayList<>();
        sql.append("select * from ").append(PprofTaskRecord.INDEX_NAME);

        final List<String> conditions = new ArrayList<>();
        if (StringUtil.isNotEmpty(serviceId)) {
            conditions.add(PprofTaskRecord.SERVICE_ID + " = ?");
            params.add(serviceId);
        }
        if (startTimeBucket != null) {
            conditions.add(PprofTaskRecord.TIME_BUCKET + " >= ?");
            params.add(startTimeBucket);
        }
        if (endTimeBucket != null) {
            conditions.add(PprofTaskRecord.TIME_BUCKET + " <= ?");
            params.add(endTimeBucket);
        }

        if (!conditions.isEmpty()) {
            sql.append(" where ");
            sql.append(String.join(" and ", conditions));
        }
        sql.append(" order by ").append(PprofTaskRecord.CREATE_TIME).append(" desc");
        if (limit != null) {
            sql.append(" limit ").append(limit);
        }

        final List<PprofTask> tasks = new ArrayList<>();
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) {
                ps.setObject(i + 1, params.get(i));
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tasks.add(buildPprofTask(rs));
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to query pprof tasks", e);
        }
        return tasks;
    }

    @Override
    public PprofTask getById(final String id) throws IOException {
        final String sql = "select * from " + PprofTaskRecord.INDEX_NAME
            + " where " + PprofTaskRecord.TASK_ID + " = ? limit 1";
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return buildPprofTask(rs);
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to get pprof task: " + id, e);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private PprofTask buildPprofTask(final ResultSet rs) throws SQLException {
        return PprofTask.builder()
            .id(rs.getString(PprofTaskRecord.TASK_ID))
            .serviceId(rs.getString(PprofTaskRecord.SERVICE_ID))
            .serviceInstanceIds(GSON.fromJson(
                rs.getString(PprofTaskRecord.SERVICE_INSTANCE_IDS), ArrayList.class))
            .createTime(rs.getLong(PprofTaskRecord.CREATE_TIME))
            .events(PprofEventType.valueOfString(
                rs.getString(PprofTaskRecord.EVENT_TYPES)))
            .duration(rs.getInt(PprofTaskRecord.DURATION))
            .dumpPeriod(rs.getInt(PprofTaskRecord.DUMP_PERIOD))
            .build();
    }
}
