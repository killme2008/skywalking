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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.oap.server.core.storage.StorageException;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.GreptimeDBSearchableTagColumns.TagColumn;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.core.storage.model.ModelColumn;
import org.apache.skywalking.oap.server.core.storage.model.ModelInstaller;
import org.apache.skywalking.oap.server.core.storage.model.StorageManipulationOpt;
import org.apache.skywalking.oap.server.library.module.ModuleManager;

@Slf4j
public class GreptimeDBTableInstaller extends ModelInstaller {
    private final GreptimeDBStorageConfig config;
    private final GreptimeDBSearchableTagColumns tagColumns;

    public GreptimeDBTableInstaller(final GreptimeDBStorageClient client,
                                    final ModuleManager moduleManager,
                                    final GreptimeDBStorageConfig config) {
        super(client, moduleManager);
        this.config = config;
        this.tagColumns = new GreptimeDBSearchableTagColumns(moduleManager, config);
    }

    @Override
    public InstallInfo isExists(final Model model, final StorageManipulationOpt opt) throws StorageException {
        final String tableName = GreptimeDBConverter.resolveTableName(model);
        final GreptimeDBInstallInfo info = new GreptimeDBInstallInfo(model);
        try (Connection conn = ((GreptimeDBStorageClient) client).getConnection()) {
            if (!tableExists(conn, tableName)) {
                info.setAllExist(false);
                opt.recordOutcome("table", tableName, StorageManipulationOpt.Outcome.MISSING, "table not found");
                return info;
            }

            // Table exists. Diff by column-name presence (matches the JDBC installer, which does not
            // compare types). A model that gained @Columns since the table was created shows up as
            // missing columns here; how we reconcile depends on the caller's schema-change intent.
            final Set<String> existing = describeColumns(conn, tableName);
            final boolean expandTags = tagColumns.expandsTags(model);
            final List<String> missing = new ArrayList<>();
            for (final ModelColumn col : model.getColumns()) {
                final String colName = col.getColumnName().getStorageName();
                if (expandTags && isTagsColumn(colName)) {
                    // The JSON tags column is intentionally not created; searchable tags are per-key columns.
                    continue;
                }
                if (!existing.contains(colName)) {
                    missing.add(colName);
                }
            }

            // Searchable tag columns whitelisted after the table was created: ALTER can only add them as
            // plain fields (no INVERTED INDEX / PRIMARY KEY), so they are queryable but unindexed until
            // the table is rebuilt. Tags whitelisted before creation keep their PK/index from createTable.
            final List<String> missingTags = new ArrayList<>();
            if (expandTags) {
                for (final TagColumn tag : tagColumns.resolve(model)) {
                    if (!existing.contains(tag.getKey())) {
                        missingTags.add(tag.getKey());
                    }
                }
            }

            // The table is present regardless, so keep allExist=true: createTable is
            // CREATE TABLE IF NOT EXISTS and would be a no-op. Column reconciliation happens here.
            info.setAllExist(true);
            final List<String> allMissing = new ArrayList<>(missing);
            allMissing.addAll(missingTags);
            if (allMissing.isEmpty()) {
                opt.recordOutcome("table", tableName, StorageManipulationOpt.Outcome.EXISTING_MATCHED, null);
            } else if (opt.isWithSchemaChange()) {
                addMissingColumns(conn, model, tableName, missing);
                addMissingTagColumns(conn, tableName, missingTags);
                opt.recordOutcome("table", tableName, StorageManipulationOpt.Outcome.UPDATED,
                    "added columns: " + String.join(", ", allMissing));
            } else {
                opt.recordOutcome("table", tableName, StorageManipulationOpt.Outcome.SKIPPED_SHAPE_MISMATCH,
                    "missing columns: " + String.join(", ", allMissing));
            }
        } catch (SQLException e) {
            throw new StorageException("Failed to check table existence: " + tableName, e);
        }
        return info;
    }

    private boolean tableExists(final Connection conn, final String tableName) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW TABLES LIKE '" + tableName + "'")) {
            return rs.next();
        }
    }

    private Set<String> describeColumns(final Connection conn, final String tableName) throws SQLException {
        final Set<String> columns = new HashSet<>();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("DESC " + tableName)) {
            while (rs.next()) {
                columns.add(rs.getString("Column"));
            }
        }
        return columns;
    }

    private void addMissingColumns(final Connection conn, final Model model, final String tableName,
                                   final List<String> missing) throws SQLException {
        for (final ModelColumn col : model.getColumns()) {
            final String colName = col.getColumnName().getStorageName();
            if (!missing.contains(colName)) {
                continue;
            }
            final String ddl = "ALTER TABLE " + tableName + " ADD COLUMN "
                + GreptimeDBConverter.quoteColumn(colName) + " " + GreptimeDBConverter.mapToSqlType(col);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(ddl);
            }
            log.info("Added column {} to GreptimeDB table {}", colName, tableName);
        }
    }

    private void addMissingTagColumns(final Connection conn, final String tableName,
                                      final List<String> tagKeys) throws SQLException {
        for (final String key : tagKeys) {
            final String ddl = "ALTER TABLE " + tableName + " ADD COLUMN "
                + GreptimeDBConverter.quoteColumn(key) + " STRING";
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(ddl);
            }
            log.info("Added searchable tag column {} to GreptimeDB table {}", key, tableName);
        }
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

        final boolean expandTags = tagColumns.expandsTags(model);
        final List<TagColumn> searchableTags = expandTags
            ? tagColumns.resolve(model) : Collections.emptyList();

        final List<String> pkColumns = new ArrayList<>(selectPrimaryKeyColumns(model));
        for (final TagColumn tag : searchableTags) {
            if (tag.isPrimaryKey()) {
                pkColumns.add(tag.getKey());
            }
        }

        final List<String> columnDefs = new ArrayList<>();

        // Synthetic id column (computed by StorageData.id().build(), not a @Column)
        columnDefs.add("  " + GreptimeDBConverter.quoteColumn("id") + " STRING");

        for (final ModelColumn col : model.getColumns()) {
            final String colName = col.getColumnName().getStorageName();
            if (expandTags && isTagsColumn(colName)) {
                // The JSON tags column is replaced by the per-key searchable tag columns below.
                continue;
            }
            final String sqlType = GreptimeDBConverter.mapToSqlType(col);
            final StringBuilder colDef = new StringBuilder();
            colDef.append("  ").append(GreptimeDBConverter.quoteColumn(colName)).append(" ").append(sqlType);

            // Index annotations
            if (!col.isStorageOnly() && !pkColumns.contains(colName)) {
                colDef.append(buildIndexClause(col, sqlType));
            }

            columnDefs.add(colDef.toString());
        }

        // Per-key searchable tag columns: PRIMARY KEY tags need no extra index (the PK prunes),
        // the rest are inverted-indexed fields.
        for (final TagColumn tag : searchableTags) {
            final StringBuilder colDef = new StringBuilder();
            colDef.append("  ").append(GreptimeDBConverter.quoteColumn(tag.getKey())).append(" STRING");
            if (!tag.isPrimaryKey()) {
                colDef.append(" INVERTED INDEX");
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

    private static boolean isTagsColumn(final String colName) {
        // SkyWalking names the searchable-tag List<String> column "tags" on every record.
        return "tags".equals(colName);
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
            // Management / NoneStream are config tables written with a constant greptime_ts
            // (see GreptimeDBConverter.MANAGEMENT_TIMESTAMP): 'forever' keeps them from ever expiring,
            // independent of any database-level default TTL. A purging TTL here would treat every
            // constant-timestamp row as already expired and wipe the configuration.
            return "forever";
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
