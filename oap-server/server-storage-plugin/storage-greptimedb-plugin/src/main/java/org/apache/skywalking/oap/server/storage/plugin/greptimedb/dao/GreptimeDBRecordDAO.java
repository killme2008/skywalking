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
import org.apache.skywalking.oap.server.core.analysis.record.Record;
import org.apache.skywalking.oap.server.core.storage.IRecordDAO;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.core.storage.type.StorageBuilder;
import org.apache.skywalking.oap.server.library.client.request.InsertRequest;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBConverter;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBStorageClient;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.SchemaRegistry;

@RequiredArgsConstructor
public class GreptimeDBRecordDAO implements IRecordDAO {
    private final GreptimeDBStorageClient client;
    private final SchemaRegistry schemaRegistry;
    @SuppressWarnings("rawtypes")
    private final StorageBuilder storageBuilder;

    @Override
    @SuppressWarnings("unchecked")
    public InsertRequest prepareBatchInsert(final Model model, final Record record) throws IOException {
        final SchemaRegistry.WriteSchemaInfo schemaInfo = schemaRegistry.getWriteSchema(model);
        final long greptimeTs = GreptimeDBConverter.timeBucketToTimestamp(
            record.getTimeBucket(), model.getDownsampling());
        final Table table = GreptimeDBTableBuilder.buildTable(
            record, storageBuilder, model, schemaInfo, greptimeTs);
        return new GreptimeDBInsertRequest(table);
    }
}
