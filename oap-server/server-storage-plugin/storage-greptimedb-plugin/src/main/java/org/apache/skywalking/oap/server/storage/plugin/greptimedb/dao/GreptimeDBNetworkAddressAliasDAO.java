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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.oap.server.core.analysis.manual.networkalias.NetworkAddressAlias;
import org.apache.skywalking.oap.server.core.storage.cache.INetworkAddressAliasDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBConverter;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBStorageClient;

@Slf4j
@RequiredArgsConstructor
public class GreptimeDBNetworkAddressAliasDAO implements INetworkAddressAliasDAO {
    private final GreptimeDBStorageClient client;

    @Override
    public List<NetworkAddressAlias> loadLastUpdate(final long timeBucket) {
        final String tableName = GreptimeDBConverter.resolveTrafficTableName(NetworkAddressAlias.INDEX_NAME);
        final String sql = "select "
            + NetworkAddressAlias.ADDRESS + ", "
            + NetworkAddressAlias.REPRESENT_SERVICE_ID + ", "
            + NetworkAddressAlias.REPRESENT_SERVICE_INSTANCE_ID + ", "
            + NetworkAddressAlias.LAST_UPDATE_TIME_BUCKET + ", "
            + NetworkAddressAlias.TIME_BUCKET
            + " from " + tableName
            + " where " + NetworkAddressAlias.LAST_UPDATE_TIME_BUCKET + " >= ?";
        try (Connection conn = client.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, timeBucket);
            try (ResultSet rs = ps.executeQuery()) {
                final List<NetworkAddressAlias> result = new ArrayList<>();
                while (rs.next()) {
                    final NetworkAddressAlias alias = new NetworkAddressAlias();
                    alias.setAddress(rs.getString(NetworkAddressAlias.ADDRESS));
                    alias.setRepresentServiceId(rs.getString(NetworkAddressAlias.REPRESENT_SERVICE_ID));
                    alias.setRepresentServiceInstanceId(
                        rs.getString(NetworkAddressAlias.REPRESENT_SERVICE_INSTANCE_ID));
                    alias.setLastUpdateTimeBucket(
                        rs.getLong(NetworkAddressAlias.LAST_UPDATE_TIME_BUCKET));
                    alias.setTimeBucket(rs.getLong(NetworkAddressAlias.TIME_BUCKET));
                    result.add(alias);
                }
                return result;
            }
        } catch (Exception e) {
            log.error("Failed to load network address aliases", e);
        }
        return Collections.emptyList();
    }
}
