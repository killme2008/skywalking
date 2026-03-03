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
import org.apache.skywalking.oap.server.core.profiling.asyncprofiler.storage.AsyncProfilerTaskLogRecord;
import org.apache.skywalking.oap.server.core.query.AsyncProfilerTaskLog;
import org.apache.skywalking.oap.server.core.query.type.AsyncProfilerTaskLogOperationType;
import org.apache.skywalking.oap.server.core.storage.profiling.asyncprofiler.IAsyncProfilerTaskLogQueryDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBStorageClient;

@RequiredArgsConstructor
public class GreptimeDBAsyncProfilerTaskLogQueryDAO implements IAsyncProfilerTaskLogQueryDAO {
    private final GreptimeDBStorageClient client;

    @Override
    public List<AsyncProfilerTaskLog> getTaskLogList() throws IOException {
        final String sql = "select * from " + AsyncProfilerTaskLogRecord.INDEX_NAME
            + " order by " + AsyncProfilerTaskLogRecord.OPERATION_TIME + " desc";
        final List<AsyncProfilerTaskLog> logs = new ArrayList<>();
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                logs.add(parseLog(rs));
            }
        } catch (SQLException e) {
            throw new IOException("Failed to query async profiler task logs", e);
        }
        return logs;
    }

    private AsyncProfilerTaskLog parseLog(final ResultSet rs) throws SQLException {
        return AsyncProfilerTaskLog.builder()
            .id(rs.getString(AsyncProfilerTaskLogRecord.TASK_ID) + "_"
                + rs.getString(AsyncProfilerTaskLogRecord.INSTANCE_ID))
            .instanceId(rs.getString(AsyncProfilerTaskLogRecord.INSTANCE_ID))
            .operationType(AsyncProfilerTaskLogOperationType.parse(
                rs.getInt(AsyncProfilerTaskLogRecord.OPERATION_TYPE)))
            .operationTime(rs.getLong(AsyncProfilerTaskLogRecord.OPERATION_TIME))
            .build();
    }
}
