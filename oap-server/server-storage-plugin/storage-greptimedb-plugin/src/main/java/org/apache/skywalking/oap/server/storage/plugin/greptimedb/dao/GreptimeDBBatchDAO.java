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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
        if (!(insertRequest instanceof GreptimeDBInsertRequest)) {
            return;
        }
        final GreptimeDBInsertRequest request = (GreptimeDBInsertRequest) insertRequest;
        final Table[] tables = groupRows(request.getRows());
        client.write(tables).whenComplete((result, throwable) -> {
            if (throwable != null) {
                log.error("Failed to write single insert to GreptimeDB", throwable);
            } else if (result == null || !result.isOk()) {
                log.error("GreptimeDB write error: {}", result == null ? "null result" : result.getErr());
            } else {
                request.onInsertCompleted();
            }
        });
    }

    @Override
    public CompletableFuture<Void> flush(final List<PrepareRequest> prepareRequests) {
        if (CollectionUtils.isEmpty(prepareRequests)) {
            return CompletableFuture.completedFuture(null);
        }

        final List<GreptimeDBPreparedRow> rows = new ArrayList<>();
        final List<GreptimeDBInsertRequest> insertCallbacks = new ArrayList<>();
        final List<GreptimeDBUpdateRequest> updateCallbacks = new ArrayList<>();
        for (final PrepareRequest request : prepareRequests) {
            if (request instanceof GreptimeDBInsertRequest) {
                final GreptimeDBInsertRequest insert = (GreptimeDBInsertRequest) request;
                rows.addAll(insert.getRows());
                insertCallbacks.add(insert);
            } else if (request instanceof GreptimeDBUpdateRequest) {
                final GreptimeDBUpdateRequest update = (GreptimeDBUpdateRequest) request;
                rows.addAll(update.getRows());
                updateCallbacks.add(update);
            }
        }
        if (rows.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        return client.write(groupRows(rows)).handle((result, throwable) -> {
            if (throwable != null) {
                log.error("Failed to flush batch to GreptimeDB", throwable);
                notifyUpdateFailure(updateCallbacks);
                throw new CompletionException(throwable);
            }
            if (result == null || !result.isOk()) {
                final Object error = result == null ? "null result" : result.getErr();
                log.error("GreptimeDB batch write error: {}", error);
                notifyUpdateFailure(updateCallbacks);
                throw new CompletionException(new IOException("GreptimeDB batch write failed: " + error));
            }
            insertCallbacks.forEach(GreptimeDBInsertRequest::onInsertCompleted);
            return null;
        });
    }

    static Table[] groupRows(final List<GreptimeDBPreparedRow> rows) {
        final Map<String, Table> grouped = new LinkedHashMap<>();
        for (final GreptimeDBPreparedRow row : rows) {
            final Table table = grouped.computeIfAbsent(
                row.getSchema().getFingerprint(),
                ignored -> Table.from(row.getSchema().getTableSchema())
            );
            table.addRow(row.getValues());
        }
        grouped.values().forEach(Table::complete);
        return grouped.values().toArray(new Table[0]);
    }

    private static void notifyUpdateFailure(final List<GreptimeDBUpdateRequest> callbacks) {
        callbacks.forEach(GreptimeDBUpdateRequest::onUpdateFailure);
    }
}
