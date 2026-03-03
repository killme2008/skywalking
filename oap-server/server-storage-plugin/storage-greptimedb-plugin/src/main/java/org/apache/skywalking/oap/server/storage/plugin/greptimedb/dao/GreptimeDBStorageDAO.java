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

import lombok.RequiredArgsConstructor;
import org.apache.skywalking.oap.server.core.storage.IManagementDAO;
import org.apache.skywalking.oap.server.core.storage.IMetricsDAO;
import org.apache.skywalking.oap.server.core.storage.INoneStreamDAO;
import org.apache.skywalking.oap.server.core.storage.IRecordDAO;
import org.apache.skywalking.oap.server.core.storage.StorageDAO;
import org.apache.skywalking.oap.server.core.storage.type.StorageBuilder;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBStorageClient;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.SchemaRegistry;

@RequiredArgsConstructor
public class GreptimeDBStorageDAO implements StorageDAO {
    private final GreptimeDBStorageClient client;
    private final SchemaRegistry schemaRegistry;

    @Override
    public IMetricsDAO newMetricsDao(final StorageBuilder storageBuilder) {
        return new GreptimeDBMetricsDAO(client, schemaRegistry, storageBuilder);
    }

    @Override
    public IRecordDAO newRecordDao(final StorageBuilder storageBuilder) {
        return new GreptimeDBRecordDAO(client, schemaRegistry, storageBuilder);
    }

    @Override
    public INoneStreamDAO newNoneStreamDao(final StorageBuilder storageBuilder) {
        return new GreptimeDBNoneStreamDAO(client, schemaRegistry, storageBuilder);
    }

    @Override
    public IManagementDAO newManagementDao(final StorageBuilder storageBuilder) {
        return new GreptimeDBManagementDAO(client, schemaRegistry, storageBuilder);
    }
}
