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
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.skywalking.oap.server.core.storage.SessionCacheCallback;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBStorageClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies that flush reflects the write outcome in its returned future, so PersistenceTimer can see
 * failures instead of silently dropping records.
 */
class GreptimeDBBatchDAOTest {
    private GreptimeDBStorageClient client;
    private GreptimeDBBatchDAO dao;

    @BeforeEach
    void setUp() {
        client = mock(GreptimeDBStorageClient.class);
        dao = new GreptimeDBBatchDAO(client);
    }

    @Test
    void flushSuccessCompletesNormallyAndFiresInsertCallback() {
        final SessionCacheCallback callback = mock(SessionCacheCallback.class);
        final GreptimeDBInsertRequest req = new GreptimeDBInsertRequest(mock(Table.class), callback);
        final Result<WriteOk, Err> ok = okResult();
        when(client.write(any(Table[].class))).thenReturn(CompletableFuture.completedFuture(ok));

        assertNull(dao.flush(Collections.singletonList(req)).join());
        verify(callback).onInsertCompleted();
    }

    @Test
    void flushWriteErrorCompletesExceptionallyAndKeepsSessionDirty() {
        final SessionCacheCallback callback = mock(SessionCacheCallback.class);
        final GreptimeDBUpdateRequest req = new GreptimeDBUpdateRequest(mock(Table.class), callback);
        final Result<WriteOk, Err> err = errResult();
        when(client.write(any(Table[].class))).thenReturn(CompletableFuture.completedFuture(err));

        assertThrows(CompletionException.class,
            () -> dao.flush(Collections.singletonList(req)).join());
        verify(callback).onUpdateFailure();
    }

    @Test
    void flushWriteExceptionPropagatesAndKeepsSessionDirty() {
        final SessionCacheCallback callback = mock(SessionCacheCallback.class);
        final GreptimeDBUpdateRequest req = new GreptimeDBUpdateRequest(mock(Table.class), callback);
        final CompletableFuture<Result<WriteOk, Err>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("boom"));
        when(client.write(any(Table[].class))).thenReturn(failed);

        assertThrows(CompletionException.class,
            () -> dao.flush(Collections.singletonList(req)).join());
        verify(callback).onUpdateFailure();
    }

    @SuppressWarnings("unchecked")
    private static Result<WriteOk, Err> okResult() {
        final Result<WriteOk, Err> result = mock(Result.class);
        when(result.isOk()).thenReturn(true);
        return result;
    }

    @SuppressWarnings("unchecked")
    private static Result<WriteOk, Err> errResult() {
        final Result<WriteOk, Err> result = mock(Result.class);
        when(result.isOk()).thenReturn(false);
        return result;
    }
}
