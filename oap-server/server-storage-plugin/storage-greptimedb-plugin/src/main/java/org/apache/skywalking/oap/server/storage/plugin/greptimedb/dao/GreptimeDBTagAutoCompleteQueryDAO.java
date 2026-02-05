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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.apache.skywalking.oap.server.core.analysis.TimeBucket;
import org.apache.skywalking.oap.server.core.analysis.manual.searchtag.TagAutocompleteData;
import org.apache.skywalking.oap.server.core.analysis.manual.searchtag.TagType;
import org.apache.skywalking.oap.server.core.query.input.Duration;
import org.apache.skywalking.oap.server.core.storage.query.ITagAutoCompleteQueryDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBStorageClient;

import static java.util.Objects.nonNull;

@RequiredArgsConstructor
public class GreptimeDBTagAutoCompleteQueryDAO implements ITagAutoCompleteQueryDAO {
    private final GreptimeDBStorageClient client;

    @Override
    public Set<String> queryTagAutocompleteKeys(final TagType tagType,
                                                final int limit,
                                                final Duration duration) throws IOException {
        final StringBuilder sql = new StringBuilder();
        final List<Object> params = new ArrayList<>();

        sql.append("select distinct ").append(TagAutocompleteData.TAG_KEY)
           .append(" from ").append(TagAutocompleteData.INDEX_NAME)
           .append(" where ").append(TagAutocompleteData.TAG_TYPE).append(" = ?");
        params.add(tagType.name());

        appendTimeBucketCondition(duration, sql, params);
        sql.append(" limit ").append(limit);

        final Set<String> keys = new HashSet<>();
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            setParameters(ps, params);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    keys.add(rs.getString(TagAutocompleteData.TAG_KEY));
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to query tag autocomplete keys", e);
        }
        return keys;
    }

    @Override
    public Set<String> queryTagAutocompleteValues(final TagType tagType,
                                                  final String tagKey,
                                                  final int limit,
                                                  final Duration duration) throws IOException {
        final StringBuilder sql = new StringBuilder();
        final List<Object> params = new ArrayList<>();

        sql.append("select ").append(TagAutocompleteData.TAG_VALUE)
           .append(" from ").append(TagAutocompleteData.INDEX_NAME)
           .append(" where ").append(TagAutocompleteData.TAG_KEY).append(" = ?");
        params.add(tagKey);
        sql.append(" and ").append(TagAutocompleteData.TAG_TYPE).append(" = ?");
        params.add(tagType.name());

        appendTimeBucketCondition(duration, sql, params);
        sql.append(" limit ").append(limit);

        final Set<String> values = new HashSet<>();
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            setParameters(ps, params);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    values.add(rs.getString(TagAutocompleteData.TAG_VALUE));
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to query tag autocomplete values", e);
        }
        return values;
    }

    private void appendTimeBucketCondition(final Duration duration,
                                           final StringBuilder sql,
                                           final List<Object> params) {
        long startTB = 0;
        long endTB = 0;
        if (nonNull(duration)) {
            startTB = TimeBucket.retainToDay4MinuteBucket(duration.getStartTimeBucketInMin());
            endTB = TimeBucket.retainToDayLastMin4MinuteBucket(duration.getEndTimeBucketInMin());
        }
        if (startTB > 0) {
            sql.append(" and ").append(TagAutocompleteData.TIME_BUCKET).append(" >= ?");
            params.add(startTB);
        }
        if (endTB > 0) {
            sql.append(" and ").append(TagAutocompleteData.TIME_BUCKET).append(" <= ?");
            params.add(endTB);
        }
    }

    private void setParameters(final PreparedStatement ps,
                               final List<Object> params) throws SQLException {
        for (int i = 0; i < params.size(); i++) {
            final Object param = params.get(i);
            if (param instanceof Long) {
                ps.setLong(i + 1, (Long) param);
            } else if (param instanceof String) {
                ps.setString(i + 1, (String) param);
            } else {
                ps.setObject(i + 1, param);
            }
        }
    }
}
