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
| Records (traces, logs) | `append_mode='true'` | Append-only for raw event data |
| Management | `merge_mode='last_row'` | Upsert by `id` for config data |
| NoneStream | `merge_mode='last_row'` | Upsert by `id` for profiling tasks |

Key design decisions:
- **Tags stored as JSON columns** on the main table, eliminating separate tag tables and JOINs. Queried via `json_path_match()`.
- **Native TTL** via `WITH ('ttl' = '...')` table options. No manual history deletion needed.
- **No date-partitioned tables**. GreptimeDB handles time-based partitioning internally via TIME INDEX.

### Prerequisites

- GreptimeDB v1.0 or later
- Ports: 4001 (gRPC), 4002 (MySQL protocol)

### Configuration

In the `application.yml` file, select the GreptimeDB storage provider:

```yaml
storage:
  selector: ${SW_STORAGE:greptimedb}
  greptimedb:
    # GreptimeDB gRPC write endpoint(s), comma-separated for multiple endpoints.
    grpcEndpoints: ${SW_STORAGE_GREPTIMEDB_GRPC_ENDPOINTS:127.0.0.1:4001}
    # GreptimeDB MySQL-compatible protocol endpoint for JDBC reads and DDL.
    jdbcHost: ${SW_STORAGE_GREPTIMEDB_JDBC_HOST:127.0.0.1}
    jdbcPort: ${SW_STORAGE_GREPTIMEDB_JDBC_PORT:4002}
    database: ${SW_STORAGE_GREPTIMEDB_DATABASE:skywalking}
    user: ${SW_STORAGE_GREPTIMEDB_USER:""}
    password: ${SW_STORAGE_GREPTIMEDB_PASSWORD:""}
    # TTL per data category. Use GreptimeDB duration format (e.g., "7d", "168h").
    metricsTTL: ${SW_STORAGE_GREPTIMEDB_METRICS_TTL:7d}
    recordsTTL: ${SW_STORAGE_GREPTIMEDB_RECORDS_TTL:3d}
    # "0" means no expiry for management data.
    managementTTL: ${SW_STORAGE_GREPTIMEDB_MANAGEMENT_TTL:0}
    maxBulkSize: ${SW_STORAGE_GREPTIMEDB_MAX_BULK_SIZE:5000}
    flushInterval: ${SW_STORAGE_GREPTIMEDB_FLUSH_INTERVAL:15}
    maxJdbcPoolSize: ${SW_STORAGE_GREPTIMEDB_MAX_JDBC_POOL_SIZE:10}
    metadataQueryMaxSize: ${SW_STORAGE_GREPTIMEDB_QUERY_MAX_SIZE:5000}
```

### Configuration Properties

| Property | Environment Variable | Default | Description |
|----------|---------------------|---------|-------------|
| `grpcEndpoints` | `SW_STORAGE_GREPTIMEDB_GRPC_ENDPOINTS` | `127.0.0.1:4001` | GreptimeDB gRPC endpoint(s) for writes. Comma-separated for multiple endpoints. |
| `jdbcHost` | `SW_STORAGE_GREPTIMEDB_JDBC_HOST` | `127.0.0.1` | GreptimeDB MySQL protocol host for reads and DDL. |
| `jdbcPort` | `SW_STORAGE_GREPTIMEDB_JDBC_PORT` | `4002` | GreptimeDB MySQL protocol port. |
| `database` | `SW_STORAGE_GREPTIMEDB_DATABASE` | `skywalking` | Database name. Created automatically if not exists. |
| `user` | `SW_STORAGE_GREPTIMEDB_USER` | `""` | Authentication username. |
| `password` | `SW_STORAGE_GREPTIMEDB_PASSWORD` | `""` | Authentication password. |
| `metricsTTL` | `SW_STORAGE_GREPTIMEDB_METRICS_TTL` | `7d` | TTL for metrics data (all downsampling levels). |
| `recordsTTL` | `SW_STORAGE_GREPTIMEDB_RECORDS_TTL` | `3d` | TTL for records (traces, logs, alarms). |
| `managementTTL` | `SW_STORAGE_GREPTIMEDB_MANAGEMENT_TTL` | `0` | TTL for management data. `0` means no expiry. |
| `maxBulkSize` | `SW_STORAGE_GREPTIMEDB_MAX_BULK_SIZE` | `5000` | Max rows per bulk write request. |
| `flushInterval` | `SW_STORAGE_GREPTIMEDB_FLUSH_INTERVAL` | `15` | Flush interval in seconds. |
| `maxJdbcPoolSize` | `SW_STORAGE_GREPTIMEDB_MAX_JDBC_POOL_SIZE` | `10` | Max JDBC connection pool size. |
| `metadataQueryMaxSize` | `SW_STORAGE_GREPTIMEDB_QUERY_MAX_SIZE` | `5000` | Max rows for metadata queries (services, instances, endpoints). |

### Running GreptimeDB

#### Docker (Standalone)

```bash
docker run -d --name greptimedb \
  -p 4000:4000 \
  -p 4001:4001 \
  -p 4002:4002 \
  greptime/greptimedb:latest standalone start
```

Port 4000 is the HTTP API (used for health checks), 4001 is gRPC, and 4002 is MySQL protocol.

#### Docker Compose

```yaml
services:
  greptimedb:
    image: greptime/greptimedb:latest
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

- **JSON column indexing**: Tag queries using `json_path_match()` rely on scanning after primary key + time range filtering. This may be slower than dedicated tag indexes for high-cardinality tag queries. GreptimeDB is working on native JSON indexing support.
- **FULLTEXT search**: Log content FULLTEXT search uses English analyzer by default.
