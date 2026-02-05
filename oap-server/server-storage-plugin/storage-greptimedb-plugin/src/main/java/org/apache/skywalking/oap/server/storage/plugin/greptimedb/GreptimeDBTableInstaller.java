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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.oap.server.core.storage.StorageException;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.core.storage.model.ModelColumn;
import org.apache.skywalking.oap.server.core.storage.model.ModelInstaller;
import org.apache.skywalking.oap.server.library.module.ModuleManager;

@Slf4j
public class GreptimeDBTableInstaller extends ModelInstaller {
    private final GreptimeDBStorageConfig config;

    public GreptimeDBTableInstaller(final GreptimeDBStorageClient client,
                                    final ModuleManager moduleManager,
                                    final GreptimeDBStorageConfig config) {
        super(client, moduleManager);
        this.config = config;
    }

    @Override
    public InstallInfo isExists(final Model model) throws StorageException {
        final String tableName = GreptimeDBConverter.resolveTableName(model);
        final GreptimeDBInstallInfo info = new GreptimeDBInstallInfo(model);
        try (Connection conn = ((GreptimeDBStorageClient) client).getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW TABLES LIKE '" + tableName + "'")) {
            info.setAllExist(rs.next());
        } catch (SQLException e) {
            throw new StorageException("Failed to check table existence: " + tableName, e);
        }
        return info;
    }

    @Override
    public void createTable(final Model model) throws StorageException {
        final String ddl = buildCreateTableDDL(model);
        log.info("Creating GreptimeDB table: {}", GreptimeDBConverter.resolveTableName(model));
        log.debug("DDL: {}", ddl);
        try {
            ((GreptimeDBStorageClient) client).executeDDL(ddl);
        } catch (SQLException e) {
            throw new StorageException("Failed to create table for model: " + model.getName(), e);
        }
    }

    String buildCreateTableDDL(final Model model) {
        final String tableName = GreptimeDBConverter.resolveTableName(model);
        final StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (\n");

        final List<String> pkColumns = selectPrimaryKeyColumns(model);
        final List<String> columnDefs = new ArrayList<>();

        // Synthetic id column (computed by StorageData.id().build(), not a @Column)
        columnDefs.add("  " + GreptimeDBConverter.quoteColumn("id") + " STRING");

        for (final ModelColumn col : model.getColumns()) {
            final String colName = col.getColumnName().getStorageName();
            final String sqlType = GreptimeDBConverter.mapToSqlType(col);
            final StringBuilder colDef = new StringBuilder();
            colDef.append("  ").append(GreptimeDBConverter.quoteColumn(colName)).append(" ").append(sqlType);

            // Index annotations
            if (!col.isStorageOnly() && !pkColumns.contains(colName)) {
                colDef.append(buildIndexClause(col, sqlType));
            }

            columnDefs.add(colDef.toString());
        }

        // TIME INDEX column
        final String quotedTs = GreptimeDBConverter.quoteColumn("greptime_ts");
        if (model.isTimeSeries()) {
            columnDefs.add("  " + quotedTs + " TIMESTAMP TIME INDEX");
        } else {
            columnDefs.add("  " + quotedTs + " TIMESTAMP DEFAULT CURRENT_TIMESTAMP() TIME INDEX");
        }

        ddl.append(String.join(",\n", columnDefs));

        // PRIMARY KEY (quote each column name)
        if (!pkColumns.isEmpty()) {
            final List<String> quotedPKs = new ArrayList<>();
            for (final String pk : pkColumns) {
                quotedPKs.add(GreptimeDBConverter.quoteColumn(pk));
            }
            ddl.append(",\n  PRIMARY KEY (").append(String.join(", ", quotedPKs)).append(")");
        }

        ddl.append("\n)");
        ddl.append(buildTableOptions(model));
        return ddl.toString();
    }

    private String buildIndexClause(final ModelColumn col, final String sqlType) {
        final String colName = col.getColumnName().getStorageName();

        // FULLTEXT index for long text content columns (log content)
        if ("STRING".equals(sqlType) && col.getLength() > 16383
            && !col.isIndexOnly()) {
            return " FULLTEXT INDEX WITH(analyzer = 'English', case_sensitive = 'false')";
        }

        // SKIPPING index for high-cardinality ID columns (trace_id, segment_id, etc.)
        if ("STRING".equals(sqlType) && isHighCardinalityColumn(colName)) {
            return " SKIPPING INDEX";
        }

        // INVERTED index for normal query columns
        if (col.shouldIndex()) {
            return " INVERTED INDEX";
        }

        return "";
    }

    private boolean isHighCardinalityColumn(final String colName) {
        return GreptimeDBConverter.isHighCardinalityColumn(colName);
    }

    List<String> selectPrimaryKeyColumns(final Model model) {
        return GreptimeDBConverter.selectPrimaryKeyColumns(model);
    }

    private String buildTableOptions(final Model model) {
        final List<String> options = new ArrayList<>();

        if (model.isRecord() && model.isTimeSeries()) {
            // True records (traces, logs, alarms) use append mode
            options.add("'append_mode' = 'true'");
        } else {
            // Metrics, Management, NoneStream (extends Record but non-timeSeries) use merge mode
            options.add("'merge_mode' = 'last_row'");
        }

        final String ttl = resolveTTL(model);
        if (ttl != null) {
            options.add("'ttl' = '" + ttl + "'");
        }

        if (options.isEmpty()) {
            return "";
        }
        return " WITH (" + String.join(", ", options) + ")";
    }

    private String resolveTTL(final Model model) {
        if (!model.isTimeSeries()) {
            // Management / NoneStream data
            final String mgmtTTL = config.getManagementTTL();
            if ("0".equals(mgmtTTL) || mgmtTTL.isEmpty()) {
                return null;
            }
            return mgmtTTL;
        }

        if (model.isMetric()) {
            return config.getMetricsTTL();
        }

        return config.getRecordsTTL();
    }

    private static class GreptimeDBInstallInfo extends InstallInfo {
        protected GreptimeDBInstallInfo(final Model model) {
            super(model);
        }

        @Override
        public String buildInstallInfoMsg() {
            return "GreptimeDB table [" + getModelName() + "] " +
                (isAllExist() ? "exists" : "not found");
        }
    }
}
