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

import com.google.common.base.Strings;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.skywalking.oap.server.core.alarm.AlarmRecord;
import org.apache.skywalking.oap.server.core.alarm.AlarmRecoveryRecord;
import org.apache.skywalking.oap.server.core.analysis.manual.searchtag.Tag;
import org.apache.skywalking.oap.server.core.query.input.Duration;
import org.apache.skywalking.oap.server.core.query.type.AlarmMessage;
import org.apache.skywalking.oap.server.core.query.type.Alarms;
import org.apache.skywalking.oap.server.core.storage.query.IAlarmQueryDAO;
import org.apache.skywalking.oap.server.library.util.CollectionUtils;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBConverter;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBStorageClient;

import static java.util.Comparator.comparing;
import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.toList;

@RequiredArgsConstructor
public class GreptimeDBAlarmQueryDAO implements IAlarmQueryDAO {
    private final GreptimeDBStorageClient client;

    @Override
    public Alarms getAlarm(final Integer scopeId, final String keyword,
                           final int limit, final int from,
                           final Duration duration, final List<Tag> tags) throws IOException {
        final StringBuilder sql = new StringBuilder();
        final List<Object> params = new ArrayList<>();

        sql.append("select * from ").append(AlarmRecord.INDEX_NAME);
        sql.append(" where 1=1");

        if (nonNull(duration)) {
            final long startTB = duration.getStartTimeBucketInSec();
            final long endTB = duration.getEndTimeBucketInSec();
            if (startTB != 0 && endTB != 0) {
                sql.append(" and ").append(AlarmRecord.TIME_BUCKET).append(" >= ?");
                params.add(startTB);
                sql.append(" and ").append(AlarmRecord.TIME_BUCKET).append(" <= ?");
                params.add(endTB);
            }
        }
        if (Objects.nonNull(scopeId)) {
            sql.append(" and ").append(AlarmRecord.SCOPE).append(" = ?");
            params.add(scopeId);
        }
        if (!Strings.isNullOrEmpty(keyword)) {
            sql.append(" and ").append(AlarmRecord.ALARM_MESSAGE).append(" like concat('%',?,'%')");
            params.add(keyword);
        }
        if (CollectionUtils.isNotEmpty(tags)) {
            for (final Tag tag : tags) {
                sql.append(" and json_path_match(").append(AlarmRecord.TAGS).append(", ?)");
                params.add(buildJsonPathMatchExpr(tag.getKey(), tag.getValue()));
            }
        }
        sql.append(" order by ").append(AlarmRecord.START_TIME).append(" desc");
        sql.append(" limit ").append(from + limit);

        final List<AlarmMessage> alarmMsgs = new ArrayList<>();
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            setParameters(ps, params);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    final Map<String, Object> map = GreptimeDBConverter.resultSetToGenericMap(rs);
                    final AlarmRecord alarmRecord = new AlarmRecord.Builder()
                        .storage2Entity(new GreptimeDBConverter.ToEntity(map));
                    final AlarmMessage alarmMessage = buildAlarmMessage(alarmRecord);
                    if (!CollectionUtils.isEmpty(alarmRecord.getTagsRawData())) {
                        // GreptimeDB stores BINARY columns directly (not Base64-encoded),
                        // so use parseDataBinary instead of parseDataBinaryBase64.
                        parseDataBinary(
                            alarmRecord.getTagsRawData(),
                            alarmMessage.getTags()
                        );
                    }
                    alarmMsgs.add(alarmMessage);
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to query alarm records", e);
        }

        final Alarms alarms = new Alarms(
            alarmMsgs.stream()
                     .sorted(comparing(AlarmMessage::getStartTime).reversed())
                     .skip(from)
                     .limit(limit)
                     .collect(toList())
        );
        updateAlarmRecoveryTime(alarms, duration);
        return alarms;
    }

    private void updateAlarmRecoveryTime(final Alarms alarms,
                                         final Duration duration) throws IOException {
        final List<AlarmMessage> alarmMessages = alarms.getMsgs();
        if (CollectionUtils.isEmpty(alarmMessages)) {
            return;
        }
        final List<String> uuids = alarmMessages.stream()
            .map(AlarmMessage::getUuid)
            .collect(toList());

        final String placeholders = uuids.stream().map(u -> "?")
            .collect(Collectors.joining(", "));
        final String sql = "select * from " + AlarmRecoveryRecord.INDEX_NAME
            + " where " + AlarmRecoveryRecord.UUID + " in (" + placeholders + ")";

        final Map<String, AlarmRecoveryRecord> recoveryMap = new HashMap<>();
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < uuids.size(); i++) {
                ps.setString(i + 1, uuids.get(i));
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    final Map<String, Object> map = GreptimeDBConverter.resultSetToGenericMap(rs);
                    final AlarmRecoveryRecord record = new AlarmRecoveryRecord.Builder()
                        .storage2Entity(new GreptimeDBConverter.ToEntity(map));
                    recoveryMap.put(record.getUuid(), record);
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to query alarm recovery records", e);
        }

        alarmMessages.forEach(msg -> {
            final AlarmRecoveryRecord recovery = recoveryMap.get(msg.getUuid());
            if (recovery != null) {
                msg.setRecoveryTime(recovery.getRecoveryTime());
            }
        });
    }

    private static String buildJsonPathMatchExpr(final String key, final String value) {
        return "$[\"" + escapeJsonPath(key) + "\"] == \"" + escapeJsonPath(value) + "\"";
    }

    private static String escapeJsonPath(final String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private void setParameters(final PreparedStatement ps,
                               final List<Object> params) throws SQLException {
        for (int i = 0; i < params.size(); i++) {
            final Object param = params.get(i);
            if (param instanceof Long) {
                ps.setLong(i + 1, (Long) param);
            } else if (param instanceof Integer) {
                ps.setInt(i + 1, (Integer) param);
            } else if (param instanceof String) {
                ps.setString(i + 1, (String) param);
            } else {
                ps.setObject(i + 1, param);
            }
        }
    }
}
