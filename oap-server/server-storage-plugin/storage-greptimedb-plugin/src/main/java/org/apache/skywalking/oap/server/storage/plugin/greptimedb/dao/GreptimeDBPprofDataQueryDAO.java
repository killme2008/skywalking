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
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.skywalking.oap.server.core.profiling.pprof.storage.PprofProfilingDataRecord;
import org.apache.skywalking.oap.server.core.storage.profiling.pprof.IPprofDataQueryDAO;
import org.apache.skywalking.oap.server.library.util.CollectionUtils;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBConverter;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBStorageClient;

@RequiredArgsConstructor
public class GreptimeDBPprofDataQueryDAO implements IPprofDataQueryDAO {
    private final GreptimeDBStorageClient client;

    @Override
    public List<PprofProfilingDataRecord> getByTaskIdAndInstances(
            final String taskId, final List<String> instanceIds) throws IOException {
        if (CollectionUtils.isEmpty(instanceIds)) {
            return new ArrayList<>();
        }
        final String placeholders = instanceIds.stream().map(id -> "?")
            .collect(Collectors.joining(","));
        final String sql = "select * from " + PprofProfilingDataRecord.INDEX_NAME
            + " where " + PprofProfilingDataRecord.TASK_ID + " = ?"
            + " and " + PprofProfilingDataRecord.INSTANCE_ID
            + " in (" + placeholders + ")";
        final List<PprofProfilingDataRecord> records = new ArrayList<>();
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            int idx = 1;
            ps.setString(idx++, taskId);
            for (final String instanceId : instanceIds) {
                ps.setString(idx++, instanceId);
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    final Map<String, Object> map =
                        GreptimeDBConverter.resultSetToGenericMap(rs);
                    records.add(new PprofProfilingDataRecord.Builder()
                        .storage2Entity(new GreptimeDBConverter.ToEntity(map)));
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to query pprof profiling data", e);
        }
        return records;
    }
}
