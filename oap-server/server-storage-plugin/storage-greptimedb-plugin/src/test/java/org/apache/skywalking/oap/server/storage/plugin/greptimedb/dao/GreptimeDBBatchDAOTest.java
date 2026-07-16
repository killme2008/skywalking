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
import io.greptime.models.DataType;
import io.greptime.models.Result;
import io.greptime.models.Table;
import io.greptime.models.TableSchema;
import io.greptime.models.WriteOk;
import java.util.Collections;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.skywalking.oap.server.core.storage.SessionCacheCallback;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBStorageClient;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.SchemaRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
        final GreptimeDBInsertRequest req = new GreptimeDBInsertRequest(
            Collections.singletonList(row()), callback);
        final Result<WriteOk, Err> ok = okResult();
        when(client.write(any(Table[].class))).thenReturn(CompletableFuture.completedFuture(ok));

        assertNull(dao.flush(Collections.singletonList(req)).join());
        verify(callback).onInsertCompleted();
    }

    @Test
    void flushWriteErrorCompletesExceptionallyAndKeepsSessionDirty() {
        final SessionCacheCallback callback = mock(SessionCacheCallback.class);
        final GreptimeDBUpdateRequest req = new GreptimeDBUpdateRequest(
            Collections.singletonList(row()), callback);
        final Result<WriteOk, Err> err = errResult();
        when(client.write(any(Table[].class))).thenReturn(CompletableFuture.completedFuture(err));

        assertThrows(CompletionException.class,
            () -> dao.flush(Collections.singletonList(req)).join());
        verify(callback).onUpdateFailure();
    }

    @Test
    void flushWriteExceptionPropagatesAndKeepsSessionDirty() {
        final SessionCacheCallback callback = mock(SessionCacheCallback.class);
        final GreptimeDBUpdateRequest req = new GreptimeDBUpdateRequest(
            Collections.singletonList(row()), callback);
        final CompletableFuture<Result<WriteOk, Err>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("boom"));
        when(client.write(any(Table[].class))).thenReturn(failed);

        assertThrows(CompletionException.class,
            () -> dao.flush(Collections.singletonList(req)).join());
        verify(callback).onUpdateFailure();
    }

    @Test
    void groupRowsShouldMergeRowsWithTheSameSchema() {
        final Table[] tables = GreptimeDBBatchDAO.groupRows(Arrays.asList(
            row("first", "table_a", "a1"),
            row("first", "table_a", "a2"),
            row("second", "table_b", "b1")
        ));

        assertEquals(2, tables.length);
        assertEquals("table_a", tables[0].tableName());
        assertEquals(2, tables[0].rowCount());
        assertEquals("table_b", tables[1].tableName());
        assertEquals(1, tables[1].rowCount());
    }

    @SuppressWarnings("unchecked")
    private static GreptimeDBPreparedRow row() {
        return row("test", "test", "value");
    }

    private static GreptimeDBPreparedRow row(final String fingerprint,
                                             final String table,
                                             final String value) {
        final SchemaRegistry.WriteSchemaInfo schema = mock(SchemaRegistry.WriteSchemaInfo.class);
        when(schema.getFingerprint()).thenReturn(fingerprint);
        when(schema.getTableSchema()).thenReturn(
            TableSchema.newBuilder(table).addField("value", DataType.String).build());
        return new GreptimeDBPreparedRow(schema, new Object[] {value});
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
