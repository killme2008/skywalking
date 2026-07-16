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

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.skywalking.oap.server.core.CoreModule;
import org.apache.skywalking.oap.server.core.alarm.AlarmRecord;
import org.apache.skywalking.oap.server.core.analysis.manual.log.LogRecord;
import org.apache.skywalking.oap.server.core.analysis.manual.segment.SegmentRecord;
import org.apache.skywalking.oap.server.core.config.ConfigService;
import org.apache.skywalking.oap.server.library.module.ModuleManager;
import org.apache.skywalking.oap.server.library.util.StringUtil;

/**
 * Reads the core searchable-tag whitelists used to validate record queries.
 *
 * <p>Tags are stored in normalized additional tables, so changing a whitelist does not change
 * the GreptimeDB schema and can take effect without rebuilding a table.</p>
 */
public class GreptimeDBSearchableTagColumns {
    private final ModuleManager moduleManager;
    private volatile ConfigService configService;

    public GreptimeDBSearchableTagColumns(final ModuleManager moduleManager) {
        this.moduleManager = moduleManager;
    }

    public Set<String> searchableKeys(final String modelName) {
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
}
