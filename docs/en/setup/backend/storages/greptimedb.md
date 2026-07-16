## GreptimeDB

[GreptimeDB](https://github.com/GreptimeTeam/greptimedb) is an open-source, cloud-native time-series database.
Activate GreptimeDB as the storage by setting the storage provider to **greptimedb**.

### Architecture

The GreptimeDB storage plugin uses a **dual-protocol** architecture:
- **gRPC** (port 4001) for high-throughput asynchronous writes via the [GreptimeDB Java Ingester SDK](https://docs.greptime.com/user-guide/ingest-data/for-iot/grpc-sdks/java)
- **MySQL protocol** (port 4002) for SQL queries and DDL via JDBC

This design maximizes write throughput (critical for APM workloads) while leveraging standard SQL for queries.

### Data Modeling

The plugin maps SkyWalking data models to GreptimeDB tables:

| SkyWalking Model | GreptimeDB Mode | Description |
|------------------|-----------------|-------------|
| Metrics | `merge_mode='last_row'` | Upsert semantics for aggregated time-series data |
| Records (traces, logs, profiling tasks) | `append_mode='true'` | Append-only for time-relative record data |
| Management | `merge_mode='last_row'` | Upsert by `id` for config data |

Key design decisions:
- **Searchable tags stored as normalized rows** in append-only additional tables. Raw `key=value`
  values use skipping indexes, and queries use correlated `EXISTS` predicates.
- **Current-state metadata stored as hourly snapshots**. These metrics retain one physical version
  per series and hour through `merge_mode='last_row'`.
- **Native TTL** via `WITH ('ttl' = '...')` table options. No manual history deletion needed.
- **No date-partitioned tables**. GreptimeDB handles time-based partitioning internally via TIME INDEX.

### Prerequisites

- GreptimeDB v1.1.2 or later
- Ports: 4001 (gRPC), 4002 (MySQL protocol)

### Configuration

In the `application.yml` file, select the GreptimeDB storage provider:

```yaml
storage:
  selector: ${SW_STORAGE:greptimedb}
  greptimedb:
    # GreptimeDB gRPC write endpoint(s), comma-separated for multiple endpoints.
    grpcEndpoints: ${SW_STORAGE_GREPTIMEDB_GRPC_ENDPOINTS:127.0.0.1:4001}
    # GreptimeDB MySQL-compatible protocol endpoint(s) for JDBC reads and DDL.
    jdbcEndpoints: ${SW_STORAGE_GREPTIMEDB_JDBC_ENDPOINTS:127.0.0.1:4002}
    database: ${SW_STORAGE_GREPTIMEDB_DATABASE:skywalking}
    user: ${SW_STORAGE_GREPTIMEDB_USER:""}
    password: ${SW_STORAGE_GREPTIMEDB_PASSWORD:""}
    # TTL per data category. Use GreptimeDB duration format (e.g., "7d", "168h").
    metricsTTL: ${SW_STORAGE_GREPTIMEDB_METRICS_TTL:7d}
    recordsTTL: ${SW_STORAGE_GREPTIMEDB_RECORDS_TTL:3d}
    maxJdbcPoolSize: ${SW_STORAGE_GREPTIMEDB_MAX_JDBC_POOL_SIZE:10}
    metadataQueryMaxSize: ${SW_STORAGE_GREPTIMEDB_QUERY_MAX_SIZE:5000}
```

### Configuration Properties

| Property | Environment Variable | Default | Description |
|----------|---------------------|---------|-------------|
| `grpcEndpoints` | `SW_STORAGE_GREPTIMEDB_GRPC_ENDPOINTS` | `127.0.0.1:4001` | GreptimeDB gRPC endpoint(s) for writes. Comma-separated for multiple endpoints. |
| `jdbcEndpoints` | `SW_STORAGE_GREPTIMEDB_JDBC_ENDPOINTS` | `127.0.0.1:4002` | GreptimeDB MySQL endpoints for reads and DDL. Comma-separated endpoints use Connector/J load balancing and failover. |
| `database` | `SW_STORAGE_GREPTIMEDB_DATABASE` | `skywalking` | Database name. Created automatically if not exists. |
| `user` | `SW_STORAGE_GREPTIMEDB_USER` | `""` | Authentication username. |
| `password` | `SW_STORAGE_GREPTIMEDB_PASSWORD` | `""` | Authentication password. |
| `metricsTTL` | `SW_STORAGE_GREPTIMEDB_METRICS_TTL` | `7d` | TTL for metrics data (all downsampling levels). |
| `recordsTTL` | `SW_STORAGE_GREPTIMEDB_RECORDS_TTL` | `3d` | TTL for records (traces, logs, alarms, profiling tasks). |
| `maxJdbcPoolSize` | `SW_STORAGE_GREPTIMEDB_MAX_JDBC_POOL_SIZE` | `10` | Max JDBC connection pool size. |
| `metadataQueryMaxSize` | `SW_STORAGE_GREPTIMEDB_QUERY_MAX_SIZE` | `5000` | Max rows for metadata queries (services, instances, endpoints). |

Searchable trace, log, and alarm tags, plus Zipkin annotation queries, are stored in append-only
additional tables. The raw `key=value` value has a GreptimeDB skipping index for exact filters.
Searchable-tag whitelist changes do not change the table schema.

For a GreptimeDB cluster, list every frontend MySQL endpoint in `jdbcEndpoints`, for example
`frontend-0:4002,frontend-1:4002,frontend-2:4002`. The plugin uses Connector/J load balancing for
both database bootstrap and the JDBC connection pool, and fails over when a frontend is unavailable.

Management data (UI templates and continuous-profiling policies) is stored with `ttl = 'forever'` and never expires, so there is no TTL to configure for it.

Tables are created automatically on OAP startup. Existing tables must match the generated column,
primary-key, index, table-mode, and TTL definitions. The plugin does not alter an incompatible schema;
drop the mismatched tables and let OAP recreate them.

### Running GreptimeDB

#### Docker (Standalone)

```bash
docker run -d --name greptimedb \
  -p 4000:4000 \
  -p 4001:4001 \
  -p 4002:4002 \
  greptime/greptimedb:v1.1.2 standalone start
```

Port 4000 is the HTTP API (used for health checks), 4001 is gRPC, and 4002 is MySQL protocol.

#### Docker Compose

```yaml
services:
  greptimedb:
    image: greptime/greptimedb:v1.1.2
    command: standalone start
    ports:
      - "4000:4000"
      - "4001:4001"
      - "4002:4002"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:4000/health"]
      interval: 5s
      timeout: 10s
      retries: 12
```

For production cluster deployment, refer to the [GreptimeDB documentation](https://docs.greptime.com/).

### Known Limitations

- **FULLTEXT search**: Log content FULLTEXT search uses English analyzer by default.
- **Trace V2 query**: SkyWalking currently exposes Trace V2 queries only with BanyanDB storage.
- **Metadata history**: Current-state metadata has hourly snapshot granularity. Minute-level
  historical presence within the same hour is not preserved.
