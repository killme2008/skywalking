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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
    // Existing table names, loaded once from information_schema. A per-model SHOW TABLES probe is
    // O(total tables) on GreptimeDB, so probing each of ~5000 models turns boot into O(n^2); a single
    // upfront listing plus O(1) membership keeps it linear.
    private volatile Set<String> existingTables;

    public GreptimeDBTableInstaller(final GreptimeDBStorageClient client,
                                    final ModuleManager moduleManager,
                                    final GreptimeDBStorageConfig config) {
        this(client, moduleManager, config, new GreptimeDBSearchableTagColumns(moduleManager, config));
    }

    public GreptimeDBTableInstaller(final GreptimeDBStorageClient client,
                                    final ModuleManager moduleManager,
                                    final GreptimeDBStorageConfig config,
                                    final GreptimeDBSearchableTagColumns tagColumns) {
        super(client, moduleManager);
        this.config = config;
        this.tagColumns = tagColumns;
    }

    @Override
    public InstallInfo isExists(final Model model, final StorageManipulationOpt opt) throws StorageException {
        final String tableName = GreptimeDBConverter.resolveTableName(model);
        final GreptimeDBInstallInfo info = new GreptimeDBInstallInfo(model);
        try (Connection conn = ((GreptimeDBStorageClient) client).getConnection()) {
            ensureExistingTablesLoaded(conn);
            if (!existingTables.contains(tableName)) {
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

            // Reconcile searchable tag columns against the current whitelist. A missing column is added
            // (field tags with their INVERTED INDEX, PK tags as plain columns with a warning — an existing
            // table's PRIMARY KEY cannot be altered). A field tag whose column already exists but has no
            // INVERTED INDEX is re-indexed: this covers a prior ADD that never got its index (partial
            // failure) and tables built by an earlier plain-column version. Index state is read from
            // information_schema.statistics, not inferred from column presence, so the reconciliation is
            // idempotent — otherwise an unindexed column would look "matched" forever.
            final List<TagColumn> missingTags = new ArrayList<>();
            final List<TagColumn> unindexedFieldTags = new ArrayList<>();
            if (expandTags) {
                final List<TagColumn> resolved = tagColumns.resolve(model);
                final Set<String> invertedIndexed = resolved.isEmpty()
                    ? Collections.emptySet() : invertedIndexedColumns(conn, tableName);
                for (final TagColumn tag : resolved) {
                    if (!existing.contains(tag.getKey())) {
                        missingTags.add(tag);
                    } else if (!tag.isPrimaryKey() && !invertedIndexed.contains(tag.getKey())) {
                        unindexedFieldTags.add(tag);
                    }
                }
            }

            // The table is present regardless, so keep allExist=true: createTable is
            // CREATE TABLE IF NOT EXISTS and would be a no-op. Column reconciliation happens here.
            info.setAllExist(true);
            final List<String> changes = new ArrayList<>(missing);
            for (final TagColumn tag : missingTags) {
                changes.add(tag.getKey());
            }
            for (final TagColumn tag : unindexedFieldTags) {
                changes.add(tag.getKey() + " (index)");
            }
            if (changes.isEmpty()) {
                opt.recordOutcome("table", tableName, StorageManipulationOpt.Outcome.EXISTING_MATCHED, null);
            } else if (opt.isWithSchemaChange()) {
                addMissingColumns(conn, model, tableName, missing);
                addMissingTagColumns(conn, tableName, missingTags);
                addMissingTagIndexes(conn, tableName, unindexedFieldTags);
                opt.recordOutcome("table", tableName, StorageManipulationOpt.Outcome.UPDATED,
                    "reconciled: " + String.join(", ", changes));
            } else {
                opt.recordOutcome("table", tableName, StorageManipulationOpt.Outcome.SKIPPED_SHAPE_MISMATCH,
                    "missing: " + String.join(", ", changes));
            }
        } catch (SQLException e) {
            throw new StorageException("Failed to check table existence: " + tableName, e);
        }
        return info;
    }

    private void ensureExistingTablesLoaded(final Connection conn) throws SQLException {
        if (existingTables != null) {
            return;
        }
        synchronized (this) {
            if (existingTables != null) {
                return;
            }
            final Set<String> loaded = ConcurrentHashMap.newKeySet();
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = ?")) {
                ps.setString(1, config.getDatabase());
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        loaded.add(rs.getString(1));
                    }
                }
            }
            existingTables = loaded;
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
                                      final List<TagColumn> tags) throws SQLException {
        for (final TagColumn tag : tags) {
            final String key = tag.getKey();
            final String quoted = GreptimeDBConverter.quoteColumn(key);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("ALTER TABLE " + tableName + " ADD COLUMN " + quoted + " STRING");
            }
            if (tag.isPrimaryKey()) {
                // GreptimeDB cannot alter an existing table's PRIMARY KEY. A tag newly promoted into
                // primaryKeyTags lands here as a plain column and only joins the PK on a rebuilt table;
                // warn loudly rather than silently degrade so the mismatch is visible.
                log.warn("Searchable tag {} is configured as a primary-key tag but was added to existing "
                    + "GreptimeDB table {} as a plain column; an existing table's PRIMARY KEY cannot be "
                    + "changed. Recreate the table to apply the new primary key.", key, tableName);
            } else {
                // Field tags carry an INVERTED INDEX at create time; reproduce it so a late-whitelisted
                // tag is indexed, not merely queryable via full scan.
                setInvertedIndex(conn, tableName, key);
            }
            log.info("Added searchable tag column {} to GreptimeDB table {}", key, tableName);
        }
    }

    private void addMissingTagIndexes(final Connection conn, final String tableName,
                                      final List<TagColumn> tags) throws SQLException {
        for (final TagColumn tag : tags) {
            setInvertedIndex(conn, tableName, tag.getKey());
            log.info("Added missing INVERTED INDEX to searchable tag column {} on GreptimeDB table {}",
                tag.getKey(), tableName);
        }
    }

    private void setInvertedIndex(final Connection conn, final String tableName,
                                  final String key) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("ALTER TABLE " + tableName + " MODIFY COLUMN "
                + GreptimeDBConverter.quoteColumn(key) + " SET INVERTED INDEX");
        }
    }

    /**
     * Columns of the table that currently carry an INVERTED INDEX, read from
     * {@code information_schema.statistics} (GreptimeDB reports the index kind in
     * {@code greptime_index_type}). Used to decide whether a searchable field tag still needs its index.
     *
     * @param conn      an open JDBC connection.
     * @param tableName the table to inspect.
     * @return the set of column names that have an inverted index.
     * @throws SQLException if the metadata query fails.
     */
    private Set<String> invertedIndexedColumns(final Connection conn,
                                               final String tableName) throws SQLException {
        final Set<String> columns = new HashSet<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT column_name FROM information_schema.statistics "
                    + "WHERE table_schema = ? AND table_name = ? AND greptime_index_type = 'INVERTED'")) {
            ps.setString(1, config.getDatabase());
            ps.setString(2, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    columns.add(rs.getString(1));
                }
            }
        }
        return columns;
    }

    @Override
    public void createTable(final Model model) throws StorageException {
        final String ddl = buildCreateTableDDL(model);
        log.info("Creating GreptimeDB table: {}", GreptimeDBConverter.resolveTableName(model));
        log.debug("DDL: {}", ddl);
        try {
            ((GreptimeDBStorageClient) client).executeDDL(ddl);
            if (existingTables != null) {
                existingTables.add(GreptimeDBConverter.resolveTableName(model));
            }
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
