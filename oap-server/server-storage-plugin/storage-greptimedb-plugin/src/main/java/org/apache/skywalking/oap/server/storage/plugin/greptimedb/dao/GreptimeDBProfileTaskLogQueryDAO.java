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
import org.apache.skywalking.oap.server.core.profiling.trace.ProfileTaskLogRecord;
import org.apache.skywalking.oap.server.core.query.type.ProfileTaskLog;
import org.apache.skywalking.oap.server.core.query.type.ProfileTaskLogOperationType;
import org.apache.skywalking.oap.server.core.storage.profiling.trace.IProfileTaskLogQueryDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBStorageClient;

@RequiredArgsConstructor
public class GreptimeDBProfileTaskLogQueryDAO implements IProfileTaskLogQueryDAO {
    private final GreptimeDBStorageClient client;

    @Override
    public List<ProfileTaskLog> getTaskLogList() throws IOException {
        final String sql = "select * from " + ProfileTaskLogRecord.INDEX_NAME
            + " order by " + ProfileTaskLogRecord.OPERATION_TIME + " desc";
        final List<ProfileTaskLog> logs = new ArrayList<>();
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                logs.add(parseLog(rs));
            }
        } catch (SQLException e) {
            throw new IOException("Failed to query profile task logs", e);
        }
        return logs;
    }

    private ProfileTaskLog parseLog(final ResultSet rs) throws SQLException {
        return ProfileTaskLog.builder()
            .id(rs.getString(ProfileTaskLogRecord.TASK_ID) + "_"
                + rs.getString(ProfileTaskLogRecord.INSTANCE_ID))
            .taskId(rs.getString(ProfileTaskLogRecord.TASK_ID))
            .instanceId(rs.getString(ProfileTaskLogRecord.INSTANCE_ID))
            .operationType(ProfileTaskLogOperationType.parse(
                rs.getInt(ProfileTaskLogRecord.OPERATION_TYPE)))
            .operationTime(rs.getLong(ProfileTaskLogRecord.OPERATION_TIME))
            .build();
    }
}
