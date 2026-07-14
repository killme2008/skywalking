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

import io.greptime.models.Err;
import io.greptime.models.Result;
import io.greptime.models.Table;
import io.greptime.models.WriteOk;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.oap.server.core.analysis.config.NoneStream;
import org.apache.skywalking.oap.server.core.storage.INoneStreamDAO;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.core.storage.type.StorageBuilder;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBConverter;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBStorageClient;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.SchemaRegistry;

@Slf4j
@RequiredArgsConstructor
public class GreptimeDBNoneStreamDAO implements INoneStreamDAO {
    private static final long WRITE_TIMEOUT_SECONDS = 30;

    private final GreptimeDBStorageClient client;
    private final SchemaRegistry schemaRegistry;
    @SuppressWarnings("rawtypes")
    private final StorageBuilder storageBuilder;

    @Override
    @SuppressWarnings("unchecked")
    public void insert(final Model model, final NoneStream noneStream) throws IOException {
        final SchemaRegistry.WriteSchemaInfo schemaInfo = schemaRegistry.getWriteSchema(model);
        // Non-time-series config: constant greptime_ts so re-inserts upsert in place (see MANAGEMENT_TIMESTAMP).
        final Table table = GreptimeDBTableBuilder.buildTable(
            noneStream, storageBuilder, model, schemaInfo, GreptimeDBConverter.MANAGEMENT_TIMESTAMP);
        table.complete();

        try {
            final Result<WriteOk, Err> result = client.write(table).get(WRITE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (result != null && !result.isOk()) {
                throw new IOException("GreptimeDB write error for " + model.getName() + ": " + result.getErr());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while inserting NoneStream to GreptimeDB: " + model.getName(), e);
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to insert NoneStream to GreptimeDB: " + model.getName(), e);
        }
    }
}
