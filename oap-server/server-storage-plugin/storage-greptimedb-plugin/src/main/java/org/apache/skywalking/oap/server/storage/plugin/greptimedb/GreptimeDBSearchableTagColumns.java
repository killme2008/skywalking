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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.skywalking.oap.server.core.CoreModule;
import org.apache.skywalking.oap.server.core.alarm.AlarmRecord;
import org.apache.skywalking.oap.server.core.analysis.manual.log.LogRecord;
import org.apache.skywalking.oap.server.core.analysis.manual.segment.SegmentRecord;
import org.apache.skywalking.oap.server.core.config.ConfigService;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.library.module.ModuleManager;
import org.apache.skywalking.oap.server.library.util.StringUtil;

/**
 * Resolves the searchable tag columns for a record model. SkyWalking keeps searchable trace/log/alarm
 * tags as a {@code List<String>} of {@code "key=value"} entries filtered by a per-type whitelist; this
 * plugin promotes each whitelisted key to its own indexed column so tag filters push down instead of
 * scanning a JSON blob. Keys listed in {@code primaryKeyTags} join the PRIMARY KEY (row-group pruning);
 * the rest become inverted-indexed fields. Models without searchable tags resolve to an empty list.
 */
public class GreptimeDBSearchableTagColumns {
    private final ModuleManager moduleManager;
    private final Set<String> primaryKeyTags;
    private final Map<String, Set<String>> whitelistSnapshots = new ConcurrentHashMap<>();
    private volatile ConfigService configService;

    public GreptimeDBSearchableTagColumns(final ModuleManager moduleManager,
                                          final GreptimeDBStorageConfig config) {
        this.moduleManager = moduleManager;
        this.primaryKeyTags = csvToOrderedSet(config.getPrimaryKeyTags());
    }

    /**
     * Whether this model stores searchable tags as per-key columns (segment / log / alarm). Independent
     * of whether the whitelist is currently empty, so the {@code tags} JSON column is dropped for these
     * models even when no tag is searchable yet (unlike Zipkin, which keeps its own tags column).
     */
    public boolean expandsTags(final Model model) {
        final String name = model.getName();
        return SegmentRecord.INDEX_NAME.equals(name)
            || LogRecord.INDEX_NAME.equals(name)
            || AlarmRecord.INDEX_NAME.equals(name);
    }

    /**
     * Searchable tag columns for the model, PRIMARY KEY tags first (in {@code primaryKeyTags} order),
     * then the field tags (sorted, for stable DDL). Empty when the model is not one of segment/log/alarm
     * or its whitelist is empty.
     */
    public List<TagColumn> resolve(final Model model) {
        final Set<String> whitelist = whitelistFor(model.getName());
        if (whitelist.isEmpty()) {
            return Collections.emptyList();
        }
        validateNoColumnCollisions(model, whitelist);
        final List<TagColumn> columns = new ArrayList<>();
        for (final String pk : primaryKeyTags) {
            if (whitelist.contains(pk)) {
                columns.add(new TagColumn(pk, true));
            }
        }
        new TreeSet<>(whitelist).stream()
            .filter(key -> !primaryKeyTags.contains(key))
            .forEach(key -> columns.add(new TagColumn(key, false)));
        return columns;
    }

    /**
     * The searchable tag keys for a table (its whitelist), used by read DAOs to reject a tag filter on a
     * non-searchable key (which has no column) — mirroring the JDBC/ES plugins.
     */
    public Set<String> searchableKeys(final String modelName) {
        return whitelistFor(modelName);
    }

    /** SkyWalking names the searchable-tag {@code List<String>} column {@code tags} on every record. */
    public static boolean isTagsColumn(final String colName) {
        return "tags".equals(colName);
    }

    private Set<String> whitelistFor(final String modelName) {
        // GreptimeDB columns are fixed by the installer. Freeze the first resolved whitelist so
        // dynamic trace-tag updates cannot make query SQL reference columns absent from that schema.
        return whitelistSnapshots.computeIfAbsent(modelName, this::loadWhitelist);
    }

    private Set<String> loadWhitelist(final String modelName) {
        final ConfigService config = configService();
        final Set<String> configured;
        switch (modelName) {
            case SegmentRecord.INDEX_NAME:
                configured = config.getSearchableTracesTags().getSearchableTags();
                break;
            case LogRecord.INDEX_NAME:
                configured = csvToOrderedSet(config.getSearchableLogsTags());
                break;
            case AlarmRecord.INDEX_NAME:
                configured = csvToOrderedSet(config.getSearchableAlarmTags());
                break;
            default:
                return Collections.emptySet();
        }
        if (configured == null || configured.isEmpty()) {
            return Collections.emptySet();
        }
        return configured.stream()
                         .map(String::trim)
                         .filter(StringUtil::isNotEmpty)
                         .collect(Collectors.collectingAndThen(
                             Collectors.toCollection(LinkedHashSet::new),
                             Collections::unmodifiableSet));
    }

    private void validateNoColumnCollisions(final Model model, final Set<String> whitelist) {
        final Set<String> reserved = model.getColumns().stream()
                                          .map(column -> column.getColumnName().getStorageName())
                                          .collect(Collectors.toCollection(TreeSet::new));
        reserved.add("id");
        reserved.add("greptime_ts");
        for (final String tag : whitelist) {
            if (reserved.contains(tag)) {
                throw new IllegalArgumentException(
                    "Searchable tag '" + tag + "' conflicts with a column in model '"
                        + model.getName() + "'");
            }
        }
    }

    private ConfigService configService() {
        if (configService == null) {
            configService = moduleManager.find(CoreModule.NAME)
                                         .provider()
                                         .getService(ConfigService.class);
        }
        return configService;
    }

    private static Set<String> csvToOrderedSet(final String csv) {
        if (StringUtil.isEmpty(csv)) {
            return Collections.emptySet();
        }
        return Arrays.stream(csv.split(","))
                     .map(String::trim)
                     .filter(StringUtil::isNotEmpty)
                     .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @Getter
    @RequiredArgsConstructor
    public static class TagColumn {
        private final String key;
        private final boolean primaryKey;
    }
}
