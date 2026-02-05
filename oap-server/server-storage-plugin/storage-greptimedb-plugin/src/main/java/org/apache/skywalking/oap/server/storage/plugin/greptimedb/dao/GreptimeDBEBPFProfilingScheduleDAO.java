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
import org.apache.skywalking.oap.server.core.profiling.ebpf.storage.EBPFProfilingScheduleRecord;
import org.apache.skywalking.oap.server.core.query.type.EBPFProfilingSchedule;
import org.apache.skywalking.oap.server.core.storage.profiling.ebpf.IEBPFProfilingScheduleDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBConverter;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBStorageClient;

@RequiredArgsConstructor
public class GreptimeDBEBPFProfilingScheduleDAO implements IEBPFProfilingScheduleDAO {
    private final GreptimeDBStorageClient client;

    @Override
    public List<EBPFProfilingSchedule> querySchedules(final String taskId) throws IOException {
        final String sql = "select * from " + EBPFProfilingScheduleRecord.INDEX_NAME
            + " where " + EBPFProfilingScheduleRecord.TASK_ID + " = ?";
        final List<EBPFProfilingSchedule> schedules = new ArrayList<>();
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, taskId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    final Map<String, Object> map = GreptimeDBConverter.resultSetToGenericMap(rs);
                    final EBPFProfilingScheduleRecord record =
                        new EBPFProfilingScheduleRecord.Builder()
                            .storage2Entity(new GreptimeDBConverter.ToEntity(map));
                    final EBPFProfilingSchedule schedule = new EBPFProfilingSchedule();
                    schedule.setScheduleId(record.getScheduleId());
                    schedule.setTaskId(record.getTaskId());
                    schedule.setProcessId(record.getProcessId());
                    schedule.setStartTime(record.getStartTime());
                    schedule.setEndTime(record.getEndTime());
                    schedules.add(schedule);
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to query eBPF profiling schedules", e);
        }
        return schedules;
    }
}
