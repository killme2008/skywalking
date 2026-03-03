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
import org.apache.skywalking.oap.server.core.profiling.asyncprofiler.storage.AsyncProfilerTaskRecord;
import org.apache.skywalking.oap.server.core.query.type.AsyncProfilerEventType;
import org.apache.skywalking.oap.server.core.query.type.AsyncProfilerTask;
import org.apache.skywalking.oap.server.core.storage.profiling.asyncprofiler.IAsyncProfilerTaskQueryDAO;
import org.apache.skywalking.oap.server.library.util.StringUtil;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBStorageClient;

import static org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBQueryHelper.addTimestampConditions;

@RequiredArgsConstructor
public class GreptimeDBAsyncProfilerTaskQueryDAO implements IAsyncProfilerTaskQueryDAO {
    private static final Gson GSON = new Gson();
    private final GreptimeDBStorageClient client;

    @Override
    public List<AsyncProfilerTask> getTaskList(final String serviceId,
                                                final Long startTimeBucket,
                                                final Long endTimeBucket,
                                                final Integer limit) throws IOException {
        final StringBuilder sql = new StringBuilder();
        final List<Object> params = new ArrayList<>();
        sql.append("select * from ").append(AsyncProfilerTaskRecord.INDEX_NAME);

        final List<String> conditions = new ArrayList<>();
        if (StringUtil.isNotEmpty(serviceId)) {
            conditions.add(AsyncProfilerTaskRecord.SERVICE_ID + " = ?");
            params.add(serviceId);
        }
        addTimestampConditions(conditions, params, startTimeBucket, endTimeBucket);

        if (!conditions.isEmpty()) {
            sql.append(" where ");
            sql.append(String.join(" and ", conditions));
        }
        sql.append(" order by ").append(AsyncProfilerTaskRecord.CREATE_TIME).append(" desc");
        if (limit != null) {
            sql.append(" limit ").append(limit);
        }

        final List<AsyncProfilerTask> tasks = new ArrayList<>();
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) {
                ps.setObject(i + 1, params.get(i));
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tasks.add(buildAsyncProfilerTask(rs));
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to query async profiler tasks", e);
        }
        return tasks;
    }

    @Override
    public AsyncProfilerTask getById(final String id) throws IOException {
        final String sql = "select * from " + AsyncProfilerTaskRecord.INDEX_NAME
            + " where " + AsyncProfilerTaskRecord.TASK_ID + " = ? limit 1";
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return buildAsyncProfilerTask(rs);
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to get async profiler task: " + id, e);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private AsyncProfilerTask buildAsyncProfilerTask(final ResultSet rs) throws SQLException {
        return AsyncProfilerTask.builder()
            .id(rs.getString(AsyncProfilerTaskRecord.TASK_ID))
            .serviceId(rs.getString(AsyncProfilerTaskRecord.SERVICE_ID))
            .serviceInstanceIds(GSON.fromJson(
                rs.getString(AsyncProfilerTaskRecord.SERVICE_INSTANCE_IDS), ArrayList.class))
            .createTime(rs.getLong(AsyncProfilerTaskRecord.CREATE_TIME))
            .events(AsyncProfilerEventType.valueOfList(
                GSON.fromJson(rs.getString(AsyncProfilerTaskRecord.EVENT_TYPES),
                    ArrayList.class)))
            .duration(rs.getInt(AsyncProfilerTaskRecord.DURATION))
            .execArgs(rs.getString(AsyncProfilerTaskRecord.EXEC_ARGS))
            .build();
    }
}
