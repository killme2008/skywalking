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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
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

        // The returned future must reflect the write outcome: PersistenceTimer relies on exceptional
        // completion to know a flush failed. Completing normally on error would silently drop records
        // (which carry no session-cache retry) and hide metric write failures.
        return client.write(tables.toArray(new Table[0])).handle((result, throwable) -> {
            if (throwable != null) {
                log.error("Failed to flush batch to GreptimeDB", throwable);
                notifyUpdateFailure(updateCallbacks);
                throw new CompletionException(throwable);
            }
            if (result == null || !result.isOk()) {
                final Object err = result == null ? "null result" : result.getErr();
                log.error("GreptimeDB batch write error: {}", err);
                notifyUpdateFailure(updateCallbacks);
                throw new CompletionException(new IOException("GreptimeDB batch write failed: " + err));
            }
            for (final GreptimeDBInsertRequest req : insertCallbacks) {
                req.onInsertCompleted();
            }
            return null;
        });
    }

    private static void notifyUpdateFailure(final List<GreptimeDBUpdateRequest> updateCallbacks) {
        // Keep dirty entries in the session cache so the next persistence cycle retries them.
        for (final GreptimeDBUpdateRequest req : updateCallbacks) {
            req.onUpdateFailure();
        }
    }
}
