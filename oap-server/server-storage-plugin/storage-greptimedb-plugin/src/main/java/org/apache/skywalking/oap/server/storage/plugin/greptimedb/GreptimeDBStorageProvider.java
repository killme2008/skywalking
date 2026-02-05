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

import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.oap.server.core.CoreModule;
import org.apache.skywalking.oap.server.core.storage.IBatchDAO;
import org.apache.skywalking.oap.server.core.storage.IHistoryDeleteDAO;
import org.apache.skywalking.oap.server.core.storage.StorageBuilderFactory;
import org.apache.skywalking.oap.server.core.storage.StorageDAO;
import org.apache.skywalking.oap.server.core.storage.StorageModule;
import org.apache.skywalking.oap.server.core.storage.cache.INetworkAddressAliasDAO;
import org.apache.skywalking.oap.server.core.storage.management.UIMenuManagementDAO;
import org.apache.skywalking.oap.server.core.storage.management.UITemplateManagementDAO;
import org.apache.skywalking.oap.server.core.storage.model.ModelCreator;
import org.apache.skywalking.oap.server.core.storage.profiling.asyncprofiler.IAsyncProfilerTaskLogQueryDAO;
import org.apache.skywalking.oap.server.core.storage.profiling.asyncprofiler.IAsyncProfilerTaskQueryDAO;
import org.apache.skywalking.oap.server.core.storage.profiling.asyncprofiler.IJFRDataQueryDAO;
import org.apache.skywalking.oap.server.core.storage.profiling.continuous.IContinuousProfilingPolicyDAO;
import org.apache.skywalking.oap.server.core.storage.profiling.ebpf.IEBPFProfilingDataDAO;
import org.apache.skywalking.oap.server.core.storage.profiling.ebpf.IEBPFProfilingScheduleDAO;
import org.apache.skywalking.oap.server.core.storage.profiling.ebpf.IEBPFProfilingTaskDAO;
import org.apache.skywalking.oap.server.core.storage.profiling.ebpf.IServiceLabelDAO;
import org.apache.skywalking.oap.server.core.storage.profiling.pprof.IPprofDataQueryDAO;
import org.apache.skywalking.oap.server.core.storage.profiling.pprof.IPprofTaskLogQueryDAO;
import org.apache.skywalking.oap.server.core.storage.profiling.pprof.IPprofTaskQueryDAO;
import org.apache.skywalking.oap.server.core.storage.profiling.trace.IProfileTaskLogQueryDAO;
import org.apache.skywalking.oap.server.core.storage.profiling.trace.IProfileTaskQueryDAO;
import org.apache.skywalking.oap.server.core.storage.profiling.trace.IProfileThreadSnapshotQueryDAO;
import org.apache.skywalking.oap.server.core.storage.query.IAggregationQueryDAO;
import org.apache.skywalking.oap.server.core.storage.query.IAlarmQueryDAO;
import org.apache.skywalking.oap.server.core.storage.query.IBrowserLogQueryDAO;
import org.apache.skywalking.oap.server.core.storage.query.IEventQueryDAO;
import org.apache.skywalking.oap.server.core.storage.query.IHierarchyQueryDAO;
import org.apache.skywalking.oap.server.core.storage.query.ILogQueryDAO;
import org.apache.skywalking.oap.server.core.storage.query.IMetadataQueryDAO;
import org.apache.skywalking.oap.server.core.storage.query.IMetricsQueryDAO;
import org.apache.skywalking.oap.server.core.storage.query.IRecordsQueryDAO;
import org.apache.skywalking.oap.server.core.storage.query.ISpanAttachedEventQueryDAO;
import org.apache.skywalking.oap.server.core.storage.query.ITagAutoCompleteQueryDAO;
import org.apache.skywalking.oap.server.core.storage.query.ITopologyQueryDAO;
import org.apache.skywalking.oap.server.core.storage.query.ITraceQueryDAO;
import org.apache.skywalking.oap.server.core.storage.query.IZipkinQueryDAO;
import org.apache.skywalking.oap.server.core.storage.ttl.StorageTTLStatusQuery;
import org.apache.skywalking.oap.server.library.module.ModuleConfig;
import org.apache.skywalking.oap.server.library.module.ModuleDefine;
import org.apache.skywalking.oap.server.library.module.ModuleProvider;
import org.apache.skywalking.oap.server.library.module.ModuleStartException;
import org.apache.skywalking.oap.server.library.module.ServiceNotProvidedException;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBAggregationQueryDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBAlarmQueryDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBAsyncProfilerTaskLogQueryDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBAsyncProfilerTaskQueryDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBBatchDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBBrowserLogQueryDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBContinuousProfilingPolicyDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBEBPFProfilingDataDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBEBPFProfilingScheduleDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBEBPFProfilingTaskDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBEventQueryDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBHierarchyQueryDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBHistoryDeleteDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBJFRDataQueryDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBLogQueryDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBMetadataQueryDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBMetricsQueryDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBNetworkAddressAliasDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBPprofDataQueryDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBPprofTaskLogQueryDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBPprofTaskQueryDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBProfileTaskLogQueryDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBProfileTaskQueryDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBProfileThreadSnapshotQueryDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBRecordsQueryDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBServiceLabelDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBSpanAttachedEventQueryDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBStorageDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBTagAutoCompleteQueryDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBTopologyQueryDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBTraceQueryDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBTTLStatusQuery;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBUIMenuManagementDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBUITemplateManagementDAO;
import org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao.GreptimeDBZipkinQueryDAO;

@Slf4j
public class GreptimeDBStorageProvider extends ModuleProvider {
    private GreptimeDBStorageConfig config;
    private GreptimeDBStorageClient client;
    private GreptimeDBTableInstaller tableInstaller;
    private SchemaRegistry schemaRegistry;

    @Override
    public String name() {
        return "greptimedb";
    }

    @Override
    public Class<? extends ModuleDefine> module() {
        return StorageModule.class;
    }

    @Override
    public ConfigCreator<? extends ModuleConfig> newConfigCreator() {
        return new ConfigCreator<GreptimeDBStorageConfig>() {
            @Override
            public Class<GreptimeDBStorageConfig> type() {
                return GreptimeDBStorageConfig.class;
            }

            @Override
            public void onInitialized(final GreptimeDBStorageConfig initialized) {
                config = initialized;
            }
        };
    }

    @Override
    public void prepare() throws ServiceNotProvidedException, ModuleStartException {
        this.client = new GreptimeDBStorageClient(config);
        this.schemaRegistry = new SchemaRegistry();
        this.tableInstaller = new GreptimeDBTableInstaller(client, getManager(), config);

        // StorageBuilderFactory: use default
        this.registerServiceImplementation(StorageBuilderFactory.class, new StorageBuilderFactory.Default());

        // Batch & Storage DAO
        this.registerServiceImplementation(IBatchDAO.class, new GreptimeDBBatchDAO(client));
        this.registerServiceImplementation(StorageDAO.class, new GreptimeDBStorageDAO(client, schemaRegistry));

        // Lifecycle
        this.registerServiceImplementation(IHistoryDeleteDAO.class, new GreptimeDBHistoryDeleteDAO());
        this.registerServiceImplementation(StorageTTLStatusQuery.class, new GreptimeDBTTLStatusQuery());

        // Cache
        this.registerServiceImplementation(INetworkAddressAliasDAO.class, new GreptimeDBNetworkAddressAliasDAO(client));

        // Query DAOs
        this.registerServiceImplementation(ITopologyQueryDAO.class, new GreptimeDBTopologyQueryDAO(client));
        this.registerServiceImplementation(IMetricsQueryDAO.class, new GreptimeDBMetricsQueryDAO(client));
        this.registerServiceImplementation(ITraceQueryDAO.class, new GreptimeDBTraceQueryDAO(client));
        this.registerServiceImplementation(IMetadataQueryDAO.class,
            new GreptimeDBMetadataQueryDAO(client, config.getMetadataQueryMaxSize()));
        this.registerServiceImplementation(IAggregationQueryDAO.class, new GreptimeDBAggregationQueryDAO(client));
        this.registerServiceImplementation(IAlarmQueryDAO.class, new GreptimeDBAlarmQueryDAO(client));
        this.registerServiceImplementation(IRecordsQueryDAO.class, new GreptimeDBRecordsQueryDAO(client));
        this.registerServiceImplementation(ILogQueryDAO.class, new GreptimeDBLogQueryDAO(client));
        this.registerServiceImplementation(IBrowserLogQueryDAO.class, new GreptimeDBBrowserLogQueryDAO(client));
        this.registerServiceImplementation(IEventQueryDAO.class, new GreptimeDBEventQueryDAO(client));
        this.registerServiceImplementation(ISpanAttachedEventQueryDAO.class, new GreptimeDBSpanAttachedEventQueryDAO(client));
        this.registerServiceImplementation(ITagAutoCompleteQueryDAO.class, new GreptimeDBTagAutoCompleteQueryDAO(client));
        this.registerServiceImplementation(IZipkinQueryDAO.class, new GreptimeDBZipkinQueryDAO(client));
        this.registerServiceImplementation(IHierarchyQueryDAO.class,
            new GreptimeDBHierarchyQueryDAO(client, config.getMetadataQueryMaxSize()));

        // Management
        this.registerServiceImplementation(UITemplateManagementDAO.class, new GreptimeDBUITemplateManagementDAO(client));
        this.registerServiceImplementation(UIMenuManagementDAO.class, new GreptimeDBUIMenuManagementDAO(client));

        // Profiling - trace
        this.registerServiceImplementation(IProfileTaskQueryDAO.class, new GreptimeDBProfileTaskQueryDAO(client));
        this.registerServiceImplementation(IProfileTaskLogQueryDAO.class, new GreptimeDBProfileTaskLogQueryDAO(client));
        this.registerServiceImplementation(IProfileThreadSnapshotQueryDAO.class, new GreptimeDBProfileThreadSnapshotQueryDAO(client));

        // Profiling - eBPF
        this.registerServiceImplementation(IEBPFProfilingTaskDAO.class, new GreptimeDBEBPFProfilingTaskDAO(client));
        this.registerServiceImplementation(IEBPFProfilingScheduleDAO.class, new GreptimeDBEBPFProfilingScheduleDAO(client));
        this.registerServiceImplementation(IEBPFProfilingDataDAO.class, new GreptimeDBEBPFProfilingDataDAO(client));
        this.registerServiceImplementation(IContinuousProfilingPolicyDAO.class, new GreptimeDBContinuousProfilingPolicyDAO(client));
        this.registerServiceImplementation(IServiceLabelDAO.class, new GreptimeDBServiceLabelDAO(client));

        // Profiling - async profiler
        this.registerServiceImplementation(IAsyncProfilerTaskQueryDAO.class, new GreptimeDBAsyncProfilerTaskQueryDAO(client));
        this.registerServiceImplementation(IAsyncProfilerTaskLogQueryDAO.class, new GreptimeDBAsyncProfilerTaskLogQueryDAO(client));
        this.registerServiceImplementation(IJFRDataQueryDAO.class, new GreptimeDBJFRDataQueryDAO(client));

        // Profiling - pprof
        this.registerServiceImplementation(IPprofTaskQueryDAO.class, new GreptimeDBPprofTaskQueryDAO(client));
        this.registerServiceImplementation(IPprofTaskLogQueryDAO.class, new GreptimeDBPprofTaskLogQueryDAO(client));
        this.registerServiceImplementation(IPprofDataQueryDAO.class, new GreptimeDBPprofDataQueryDAO(client));
    }

    @Override
    public void start() throws ServiceNotProvidedException, ModuleStartException {
        try {
            client.connect();

            final ModelCreator modelCreator = getManager().find(CoreModule.NAME)
                                                          .provider()
                                                          .getService(ModelCreator.class);
            modelCreator.addModelListener(tableInstaller);
        } catch (Exception e) {
            throw new ModuleStartException("Failed to connect to GreptimeDB", e);
        }
    }

    @Override
    public void notifyAfterCompleted() throws ServiceNotProvidedException, ModuleStartException {
    }

    @Override
    public String[] requiredModules() {
        return new String[] {CoreModule.NAME};
    }
}
