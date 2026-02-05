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
import org.apache.skywalking.oap.server.core.profiling.trace.ProfileThreadSnapshotRecord;
import org.apache.skywalking.oap.server.core.storage.profiling.trace.IProfileThreadSnapshotQueryDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBConverter;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBStorageClient;

@RequiredArgsConstructor
public class GreptimeDBProfileThreadSnapshotQueryDAO implements IProfileThreadSnapshotQueryDAO {
    private final GreptimeDBStorageClient client;

    @Override
    public List<ProfileThreadSnapshotRecord> queryRecords(final String taskId) throws IOException {
        final String sql = "select * from " + ProfileThreadSnapshotRecord.INDEX_NAME
            + " where " + ProfileThreadSnapshotRecord.TASK_ID + " = ?"
            + " and " + ProfileThreadSnapshotRecord.SEQUENCE + " = 0";
        final List<ProfileThreadSnapshotRecord> records = new ArrayList<>();
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, taskId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    records.add(buildRecord(rs));
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to query profile thread snapshots by task", e);
        }
        return records;
    }

    @Override
    public int queryMinSequence(final String segmentId, final long start,
                                final long end) throws IOException {
        return querySequence(segmentId, start, end, "MIN");
    }

    @Override
    public int queryMaxSequence(final String segmentId, final long start,
                                final long end) throws IOException {
        return querySequence(segmentId, start, end, "MAX");
    }

    @Override
    public List<ProfileThreadSnapshotRecord> queryRecords(final String segmentId,
                                                           final int minSequence,
                                                           final int maxSequence) throws IOException {
        final String sql = "select * from " + ProfileThreadSnapshotRecord.INDEX_NAME
            + " where " + ProfileThreadSnapshotRecord.SEGMENT_ID + " = ?"
            + " and " + ProfileThreadSnapshotRecord.SEQUENCE + " >= ?"
            + " and " + ProfileThreadSnapshotRecord.SEQUENCE + " < ?"
            + " order by " + ProfileThreadSnapshotRecord.SEQUENCE + " asc";
        final List<ProfileThreadSnapshotRecord> records = new ArrayList<>();
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, segmentId);
            ps.setInt(2, minSequence);
            ps.setInt(3, maxSequence);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    records.add(buildRecord(rs));
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to query profile thread snapshots by segment", e);
        }
        return records;
    }

    private int querySequence(final String segmentId, final long start,
                               final long end, final String aggregate) throws IOException {
        final String sql = "select " + aggregate + "(" + ProfileThreadSnapshotRecord.SEQUENCE + ")"
            + " from " + ProfileThreadSnapshotRecord.INDEX_NAME
            + " where " + ProfileThreadSnapshotRecord.SEGMENT_ID + " = ?"
            + " and " + ProfileThreadSnapshotRecord.DUMP_TIME + " >= ?"
            + " and " + ProfileThreadSnapshotRecord.DUMP_TIME + " <= ?";
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, segmentId);
            ps.setLong(2, start);
            ps.setLong(3, end);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to query " + aggregate + " sequence", e);
        }
        return -1;
    }

    private ProfileThreadSnapshotRecord buildRecord(final ResultSet rs) throws SQLException {
        final Map<String, Object> map = GreptimeDBConverter.resultSetToGenericMap(rs);
        return new ProfileThreadSnapshotRecord.Builder()
            .storage2Entity(new GreptimeDBConverter.ToEntity(map));
    }
}
