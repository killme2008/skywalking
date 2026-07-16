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

package org.apache.skywalking.oap.server.storage.plugin.greptimedb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.oap.server.core.storage.StorageException;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.core.storage.model.ModelInstaller;
import org.apache.skywalking.oap.server.core.storage.model.StorageManipulationOpt;
import org.apache.skywalking.oap.server.library.module.ModuleManager;

@Slf4j
public class GreptimeDBTableInstaller extends ModelInstaller {
    private final GreptimeDBStorageConfig config;
    private final SchemaRegistry schemaRegistry;
    private volatile Set<String> existingTables;

    public GreptimeDBTableInstaller(final GreptimeDBStorageClient client,
                                    final ModuleManager moduleManager,
                                    final GreptimeDBStorageConfig config,
                                    final SchemaRegistry schemaRegistry) {
        super(client, moduleManager);
        this.config = config;
        this.schemaRegistry = schemaRegistry;
    }

    @Override
    public InstallInfo isExists(final Model model, final StorageManipulationOpt opt) throws StorageException {
        final GreptimeDBInstallInfo info = new GreptimeDBInstallInfo(model);
        final Set<String> tables = existingTables();
        boolean allExist = true;
        for (final GreptimeDBTableSchema schema : schemasFor(model)) {
            if (tables.contains(schema.getTableName())) {
                opt.recordOutcome("table", schema.getTableName(),
                    StorageManipulationOpt.Outcome.EXISTING_MATCHED, null);
            } else {
                allExist = false;
                opt.recordOutcome("table", schema.getTableName(),
                    StorageManipulationOpt.Outcome.MISSING, "table not found");
            }
        }
        info.setAllExist(allExist);
        return info;
    }

    @Override
    public void createTable(final Model model) throws StorageException {
        for (final GreptimeDBTableSchema schema : schemasFor(model)) {
            final String ddl = schema.buildCreateTableDDL();
            log.info("Creating GreptimeDB table: {}", schema.getTableName());
            log.debug("DDL: {}", ddl);
            try {
                ((GreptimeDBStorageClient) client).executeDDL(ddl);
                final Set<String> tables = existingTables;
                if (tables != null) {
                    tables.add(schema.getTableName());
                }
            } catch (SQLException e) {
                throw new StorageException("Failed to create GreptimeDB table " + schema.getTableName(), e);
            }
        }
    }

    String buildCreateTableDDL(final Model model) {
        return schemaRegistry.getWriteSchema(model).getSchema().buildCreateTableDDL();
    }

    List<String> selectPrimaryKeyColumns(final Model model) {
        return GreptimeDBConverter.selectPrimaryKeyColumns(model);
    }

    private List<GreptimeDBTableSchema> schemasFor(final Model model) {
        return schemaRegistry.getWriteSchemas(model).stream()
            .map(SchemaRegistry.WriteSchemaInfo::getSchema)
            .collect(Collectors.toList());
    }

    private Set<String> existingTables() throws StorageException {
        Set<String> tables = existingTables;
        if (tables != null) {
            return tables;
        }
        synchronized (this) {
            tables = existingTables;
            if (tables != null) {
                return tables;
            }
            final Set<String> loaded = ConcurrentHashMap.newKeySet();
            try (Connection connection = ((GreptimeDBStorageClient) client).getConnection();
                 PreparedStatement statement = connection.prepareStatement(
                     "SELECT table_name FROM information_schema.tables WHERE table_schema = ?")) {
                statement.setString(1, config.getDatabase());
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        loaded.add(resultSet.getString(1));
                    }
                }
            } catch (SQLException e) {
                throw new StorageException("Failed to list GreptimeDB tables", e);
            }
            existingTables = loaded;
            return loaded;
        }
    }

    private static class GreptimeDBInstallInfo extends InstallInfo {
        private GreptimeDBInstallInfo(final Model model) {
            super(model);
        }

        @Override
        public String buildInstallInfoMsg() {
            return "GreptimeDB table [" + getModelName() + "] "
                + (isAllExist() ? "exists" : "not found");
        }
    }
}
