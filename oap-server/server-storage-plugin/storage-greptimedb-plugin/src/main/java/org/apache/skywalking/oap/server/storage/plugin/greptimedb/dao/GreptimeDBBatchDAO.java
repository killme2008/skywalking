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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.oap.server.core.storage.IBatchDAO;
import org.apache.skywalking.oap.server.library.client.request.InsertRequest;
import org.apache.skywalking.oap.server.library.client.request.PrepareRequest;
import org.apache.skywalking.oap.server.library.util.CollectionUtils;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBStorageClient;

@Slf4j
@RequiredArgsConstructor
public class GreptimeDBBatchDAO implements IBatchDAO {
    private final GreptimeDBStorageClient client;

    @Override
    public void insert(final InsertRequest insertRequest) {
        if (insertRequest instanceof GreptimeDBInsertRequest) {
            final GreptimeDBInsertRequest req = (GreptimeDBInsertRequest) insertRequest;
            final Table table = req.getTable();
            table.complete();
            client.write(table).whenComplete((result, throwable) -> {
                if (throwable != null) {
                    log.error("Failed to write single insert to GreptimeDB", throwable);
                } else if (result != null && !result.isOk()) {
                    log.error("GreptimeDB write error: {}", result.getErr());
                } else {
                    req.onInsertCompleted();
                }
            });
        }
    }

    @Override
    public CompletableFuture<Void> flush(final List<PrepareRequest> prepareRequests) {
        if (CollectionUtils.isEmpty(prepareRequests)) {
            return CompletableFuture.completedFuture(null);
        }

        final List<Table> tables = new ArrayList<>(prepareRequests.size());
        final List<GreptimeDBInsertRequest> insertCallbacks = new ArrayList<>();
        final List<GreptimeDBUpdateRequest> updateCallbacks = new ArrayList<>();

        for (final PrepareRequest request : prepareRequests) {
            Table table = null;
            if (request instanceof GreptimeDBInsertRequest) {
                final GreptimeDBInsertRequest insertReq = (GreptimeDBInsertRequest) request;
                table = insertReq.getTable();
                insertCallbacks.add(insertReq);
            } else if (request instanceof GreptimeDBUpdateRequest) {
                final GreptimeDBUpdateRequest updateReq = (GreptimeDBUpdateRequest) request;
                table = updateReq.getTable();
                updateCallbacks.add(updateReq);
            }
            if (table != null) {
                table.complete();
                tables.add(table);
            }
        }

        if (tables.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        return client.write(tables.toArray(new Table[0])).thenAccept(result -> {
            if (result != null && result.isOk()) {
                for (final GreptimeDBInsertRequest req : insertCallbacks) {
                    req.onInsertCompleted();
                }
            } else {
                if (result != null) {
                    log.error("GreptimeDB batch write error: {}", result.getErr());
                }
                // Notify update requests so session cache knows to retry
                for (final GreptimeDBUpdateRequest req : updateCallbacks) {
                    req.onUpdateFailure();
                }
            }
        }).exceptionally(throwable -> {
            log.error("Failed to flush batch to GreptimeDB", throwable);
            for (final GreptimeDBUpdateRequest req : updateCallbacks) {
                req.onUpdateFailure();
            }
            return null;
        });
    }
}
