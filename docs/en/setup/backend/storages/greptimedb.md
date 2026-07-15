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
- **Searchable tags stored as per-key columns** with primary-key or inverted indexes, eliminating separate tag tables and JOINs.
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
    maxJdbcPoolSize: ${SW_STORAGE_GREPTIMEDB_MAX_JDBC_POOL_SIZE:10}
    metadataQueryMaxSize: ${SW_STORAGE_GREPTIMEDB_QUERY_MAX_SIZE:5000}
    primaryKeyTags: ${SW_STORAGE_GREPTIMEDB_PRIMARY_KEY_TAGS:http.method,status_code}
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
| `maxJdbcPoolSize` | `SW_STORAGE_GREPTIMEDB_MAX_JDBC_POOL_SIZE` | `10` | Max JDBC connection pool size. |
| `metadataQueryMaxSize` | `SW_STORAGE_GREPTIMEDB_QUERY_MAX_SIZE` | `5000` | Max rows for metadata queries (services, instances, endpoints). |
| `primaryKeyTags` | `SW_STORAGE_GREPTIMEDB_PRIMARY_KEY_TAGS` | `http.method,status_code` | Subset of the searchable tag keys promoted into a record table's PRIMARY KEY. Must be low cardinality; the remaining searchable tags become inverted-indexed field columns. |

Searchable trace/log/alarm tags (the `searchableTracesTags` / `searchableLogsTags` / `searchableAlarmTags` core config) are stored as per-key indexed columns rather than a JSON blob, so tag filters push down. Keys listed in `primaryKeyTags` join the table's PRIMARY KEY; the rest become inverted-indexed fields.

The searchable-tag list is fixed when OAP installs the GreptimeDB schema. Restart OAP after changing these settings so the installer can add the new columns before queries use them: a newly whitelisted field tag is added with its inverted index (`ALTER TABLE ... MODIFY COLUMN ... SET INVERTED INDEX`), so it is indexed, not merely queryable. A tag newly added to `primaryKeyTags`, however, only lands as a plain column on an existing table — GreptimeDB cannot change an existing table's PRIMARY KEY, so recreate the table to apply a new primary-key tag. Searchable tag names must not collide with model columns or the reserved `id` and `greptime_ts` columns.

Management data (UI templates and continuous-profiling policies) is stored with `ttl = 'forever'` and never expires, so there is no TTL to configure for it.

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

- **Dynamic searchable-tag updates**: Changes take effect after an OAP restart. New field tags are added to existing tables with an inverted index, but a new `primaryKeyTags` entry does not change an existing table's PRIMARY KEY — recreate the table to apply it.
- **FULLTEXT search**: Log content FULLTEXT search uses English analyzer by default.
