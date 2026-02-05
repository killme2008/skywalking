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

import io.greptime.models.Table;
import java.io.IOException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.oap.server.core.analysis.management.ManagementData;
import org.apache.skywalking.oap.server.core.storage.IManagementDAO;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.core.storage.type.StorageBuilder;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBStorageClient;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.SchemaRegistry;

@Slf4j
@RequiredArgsConstructor
public class GreptimeDBManagementDAO implements IManagementDAO {
    private final GreptimeDBStorageClient client;
    private final SchemaRegistry schemaRegistry;
    @SuppressWarnings("rawtypes")
    private final StorageBuilder storageBuilder;

    @Override
    @SuppressWarnings("unchecked")
    public void insert(final Model model, final ManagementData storageData) throws IOException {
        final SchemaRegistry.WriteSchemaInfo schemaInfo = schemaRegistry.getWriteSchema(model);
        // Management data is not time-series, use current time for greptime_ts
        final long greptimeTs = System.currentTimeMillis();
        final Table table = GreptimeDBTableBuilder.buildTable(
            storageData, storageBuilder, model, schemaInfo, greptimeTs);
        table.complete();

        try {
            client.write(table).get();
        } catch (Exception e) {
            throw new IOException("Failed to insert ManagementData to GreptimeDB: " + model.getName(), e);
        }
    }
}
