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
import lombok.RequiredArgsConstructor;
import org.apache.skywalking.oap.server.core.alarm.AlarmRecord;
import org.apache.skywalking.oap.server.core.alarm.AlarmRecoveryRecord;
import org.apache.skywalking.oap.server.core.analysis.Layer;
import org.apache.skywalking.oap.server.core.analysis.manual.searchtag.Tag;
import org.apache.skywalking.oap.server.core.query.input.AlarmQueryCondition;
import org.apache.skywalking.oap.server.core.query.input.Duration;
import org.apache.skywalking.oap.server.core.query.input.EntityIdConstraint;
import org.apache.skywalking.oap.server.core.query.type.AlarmMessage;
import org.apache.skywalking.oap.server.core.query.type.Alarms;
import org.apache.skywalking.oap.server.core.storage.query.IAlarmQueryDAO;
import org.apache.skywalking.oap.server.library.util.CollectionUtils;
import org.apache.skywalking.oap.server.library.util.StringUtil;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBConverter;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBStorageClient;

import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBQueryHelper.appendTimestampCondition;
import static org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBQueryHelper.buildJsonPathMatchExpr;
import static org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBQueryHelper.setParameters;

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
                appendTimestampCondition(sql, params, startTB, endTB);
            }
        }
        if (nonNull(scopeId)) {
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
            alarmMsgs.stream()                     .skip(from)
                     .limit(limit)
                     .collect(toList())
        );
        updateAlarmRecoveryTime(alarms, duration);
        return alarms;
    }

    @Override
    public Alarms queryAlarms(final AlarmQueryCondition condition,
                              final int limit, final int from) throws IOException {
        if (condition == null || condition.getDuration() == null) {
            return new Alarms();
        }
        final Duration duration = condition.getDuration();
        // Non-searchable-tag guard is unnecessary here: GreptimeDB stores every tag in the JSON
        // `tags` column, so json_path_match can query any tag key (unlike JDBC/ES, where only
        // configured searchable tags get their own indexed column).
        final List<EntityIdConstraint> entityConstraints = resolveEntityFilters(condition.getEntities());

        final StringBuilder sql = new StringBuilder();
        final List<Object> params = new ArrayList<>();
        sql.append("select * from ").append(AlarmRecord.INDEX_NAME).append(" where 1=1");

        final long startTB = duration.getStartTimeBucketInSec();
        final long endTB = duration.getEndTimeBucketInSec();
        if (startTB != 0 && endTB != 0) {
            appendTimestampCondition(sql, params, startTB, endTB);
        }
        if (!Strings.isNullOrEmpty(condition.getKeyword())) {
            sql.append(" and ").append(AlarmRecord.ALARM_MESSAGE).append(" like concat('%',?,'%')");
            params.add(condition.getKeyword());
        }
        if (StringUtil.isNotEmpty(condition.getLayer())) {
            sql.append(" and ").append(AlarmRecord.LAYER).append(" = ?");
            params.add(Layer.nameOf(condition.getLayer()).value());
        }
        if (CollectionUtils.isNotEmpty(condition.getRuleNames())) {
            sql.append(" and ").append(AlarmRecord.RULE_NAME).append(" in (")
               .append(condition.getRuleNames().stream().map(it -> "?").collect(joining(", ")))
               .append(")");
            params.addAll(condition.getRuleNames());
        }
        if (CollectionUtils.isNotEmpty(entityConstraints)) {
            // Per-entity constraints OR together; within each, the id0 / id1 predicates AND together.
            final List<String> clauses = new ArrayList<>(entityConstraints.size());
            for (final EntityIdConstraint c : entityConstraints) {
                final List<String> parts = new ArrayList<>(2);
                if (c.getId0() != null) {
                    parts.add(AlarmRecord.ID0 + " = ?");
                    params.add(c.getId0());
                }
                if (c.getId1() != null) {
                    parts.add(AlarmRecord.ID1 + " = ?");
                    params.add(c.getId1());
                }
                if (!parts.isEmpty()) {
                    clauses.add("(" + String.join(" and ", parts) + ")");
                }
            }
            if (!clauses.isEmpty()) {
                sql.append(" and (").append(String.join(" or ", clauses)).append(")");
            }
        }
        if (CollectionUtils.isNotEmpty(condition.getTags())) {
            for (final Tag tag : condition.getTags()) {
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
                        parseDataBinary(alarmRecord.getTagsRawData(), alarmMessage.getTags());
                    }
                    alarmMsgs.add(alarmMessage);
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to query alarm records", e);
        }

        final Alarms alarms = new Alarms(
            alarmMsgs.stream()                     .skip(from)
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
            .collect(joining(", "));
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

}
