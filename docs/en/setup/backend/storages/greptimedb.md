## GreptimeDB

<p align="center">
  <a href="greptimedb.md">English</a> | <a href="../../../../zh/setup/backend/storages/greptimedb.md">简体中文</a>
</p>

[GreptimeDB](https://github.com/GreptimeTeam/greptimedb) can be used as the SkyWalking OAP storage backend by setting the storage provider to `greptimedb`.

### Availability

The plugin documented here is available in the unofficial downstream release [`v11.0.0-greptimedb.1`](https://github.com/killme2008/skywalking/releases/tag/v11.0.0-greptimedb.1). It is based on Apache SkyWalking `11.0.0-SNAPSHOT` at upstream commit [`46129f18`](https://github.com/apache/skywalking/commit/46129f18e815829ea14afce9a013bae7d8dfdc66).

The plugin is not part of an Apache Software Foundation release. The official `apache/skywalking-oap-server` image does not contain it; use the downstream image shown in the quick start below.

### Upstream proposal

Official GreptimeDB storage support is being discussed in [apache/skywalking discussion #13722](https://github.com/apache/skywalking/discussions/13722). If you want to use GreptimeDB with SkyWalking, please add your use case there. Data volume, retention, deployment model, and required query features give the maintainers more useful input than a simple `+1`.

### Supported scope

| Area | Scope |
| --- | --- |
| Metrics | SkyWalking metrics ingestion and query, with last-row merge semantics and native TTL. |
| Records | Traces, logs, alarms, events, browser error logs, and Zipkin data. Records are append-only with configurable TTL. |
| Search | Exact filters on searchable trace, log, and alarm tags and Zipkin annotations. Log keyword search uses `matches_term`. |
| Profiling | Trace profiling, async-profiler, eBPF profiling, pprof, JFR data, and span-attached events. |
| Management data | UI templates, runtime rules, network address aliases, service labels, and continuous-profiling policies. |
| Cluster access | Multiple gRPC write endpoints; multiple JDBC frontend endpoints with Connector/J load balancing and failover. |

GreptimeDB-specific E2E cases cover core storage, logs, alarms, Zipkin, trace profiling, and pprof. Other registered DAOs are covered by unit tests or the shared storage behavior but do not all have a dedicated GreptimeDB E2E case.

### Prerequisites

- GreptimeDB v1.1.2 or later.
- MySQL Connector/J. The driver is not included in the SkyWalking source, binary distribution, or downstream image.
- A user that can connect to the `public` database, create the configured database and tables, and read and write the configured database.

MySQL Connector/J is released under [GPLv2 with the Universal FOSS Exception](https://github.com/mysql/mysql-connector-j/blob/release/8.x/LICENSE). Apache classifies GPL dependencies, including most exceptions, as [Category X](https://www.apache.org/legal/resolved.html#category-x) and does not allow them in ASF distributions. This downstream build follows the same distribution rule, so users must obtain the driver separately.

The E2E suite currently uses Connector/J 8.0.13. Other versions have not been validated by this project.

### Docker quick start

This example starts a disposable GreptimeDB instance and the downstream OAP image on the same Docker network. It stores GreptimeDB data inside the container; removing the container removes the data.

Download the Connector/J version used by the E2E suite:

```bash
export MYSQL_CONNECTOR_VERSION=8.0.13
export MYSQL_CONNECTOR_J="${PWD}/mysql-connector-java-${MYSQL_CONNECTOR_VERSION}.jar"

curl --fail --location \
  --output "${MYSQL_CONNECTOR_J}" \
  "https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/${MYSQL_CONNECTOR_VERSION}/mysql-connector-java-${MYSQL_CONNECTOR_VERSION}.jar"
```

Create a Docker network and start GreptimeDB:

```bash
docker network create skywalking-greptimedb

docker run -d \
  --name greptimedb \
  --network skywalking-greptimedb \
  -p 4000:4000 \
  -p 4001:4001 \
  -p 4002:4002 \
  greptime/greptimedb:v1.1.2 \
  standalone start \
  --http-addr=0.0.0.0:4000 \
  --rpc-bind-addr=0.0.0.0:4001 \
  --mysql-addr=0.0.0.0:4002

for attempt in {1..60}; do
  if curl --fail --silent http://127.0.0.1:4000/health > /dev/null; then
    break
  fi
  if [ "${attempt}" -eq 60 ]; then
    echo "GreptimeDB did not become healthy" >&2
    docker logs greptimedb
    exit 1
  fi
  sleep 2
done
```

Start OAP with GreptimeDB storage enabled:

```bash
docker run -d \
  --name skywalking-oap \
  --network skywalking-greptimedb \
  -p 11800:11800 \
  -p 12800:12800 \
  -p 9411:9411 \
  -v "${MYSQL_CONNECTOR_J}:/skywalking/ext-libs/mysql-connector-j.jar:ro" \
  -e SW_STORAGE=greptimedb \
  -e SW_STORAGE_GREPTIMEDB_GRPC_ENDPOINTS=greptimedb:4001 \
  -e SW_STORAGE_GREPTIMEDB_JDBC_ENDPOINTS=greptimedb:4002 \
  -e SW_STORAGE_GREPTIMEDB_DATABASE=skywalking \
  -e SW_HEALTH_CHECKER=default \
  -e SW_RECEIVER_ZIPKIN=default \
  -e SW_QUERY_ZIPKIN=default \
  -e "JAVA_OPTS=-Xms1g -Xmx1g" \
  ghcr.io/killme2008/greptimedb-oap:11.0.0-greptimedb.1
```

OAP creates the `skywalking` database and its tables during startup. Wait for the health endpoint to return success:

```bash
for attempt in {1..60}; do
  if curl --fail --silent http://127.0.0.1:12800/healthcheck > /dev/null; then
    break
  fi
  if [ "${attempt}" -eq 60 ]; then
    echo "OAP did not become healthy" >&2
    docker logs skywalking-oap
    exit 1
  fi
  sleep 5
done
```

#### Start Horizon UI

Create a standalone Horizon config in any directory. The OAP hostname must match
the OAP container name on the Docker network:

```bash
export HORIZON_CONFIG="${PWD}/horizon-greptimedb.yaml"

cat > "${HORIZON_CONFIG}" <<'EOF'
server:
  host: 0.0.0.0
  port: 8081

oap:
  queryUrl: http://skywalking-oap:12800
  adminUrl: http://skywalking-oap:17128
  zipkinUrl: http://skywalking-oap:9412/zipkin

auth:
  backend: local
  local:
    users:
      - username: admin
        passwordHash: "$argon2id$v=19$m=65536,t=3,p=4$joV9AVlyLS3pqq4mLrYokQ$pJLkTKrz9/LzEH6YaFljdz9k8dyBiryjwSB26Diiz9U"
        roles: [admin]
EOF
```

Start the official Horizon UI on the same Docker network:

```bash
docker run -d \
  --name skywalking-ui \
  --network skywalking-greptimedb \
  -p 8080:8081 \
  -v "${HORIZON_CONFIG}:/app/horizon.yaml:ro" \
  apache/skywalking-ui:latest
```

Open <http://localhost:8080> and sign in with `admin` / `admin`. This account is
for local testing only. Change it before exposing the UI outside your machine.

Port `11800` receives agent telemetry. Port `12800` serves the OAP query API and
health endpoint; it is not the web UI. See [UI setup](../ui-setup.md) for Horizon
configuration and deployment details.

#### Verify telemetry in Horizon

Point a SkyWalking agent at `127.0.0.1:11800` and send traffic to the instrumented
service. Metrics are aggregated by minute, so allow one or two minutes for every
dashboard to populate. The screenshots below were captured from a smoke workload
with a consumer calling a provider that accesses H2.

The services dashboard summarizes active services, RPM, latency, SLA, and the
current topology:

![Services dashboard](../images/greptimedb/services-dashboard.png)

Select a service to inspect its traffic, error rate, Apdex, response-time
percentiles, and instance metrics:

![Service metrics](../images/greptimedb/service-metrics.png)

The topology view should contain both services and their database dependency:

![Topology](../images/greptimedb/topology.png)

The trace page should continuously return successful segments while traffic is
running:

![Traces](../images/greptimedb/traces.png)

Open a segment to verify the cross-service reference and database spans:

![Trace detail](../images/greptimedb/trace-detail.png)

If the agent reports application logs, the Logs page should return them with
trace links where a trace ID is present:

![Logs](../images/greptimedb/logs.png)

The smoke workload shown here takes more than one second per request, which
triggers SkyWalking's default response-time alarms. The Alarms page verifies
that alarm records are also written to and queried from GreptimeDB:

![Alarms](../images/greptimedb/alarms.png)

If OAP does not become healthy, inspect its startup log:

```bash
docker logs skywalking-oap
```

For a binary distribution, copy the Connector/J jar to `oap-libs` instead of mounting it at `/skywalking/ext-libs`.

### Configuration

Select the GreptimeDB storage provider in `application.yml`:

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
    # TTL per data category. Use GreptimeDB duration format, for example "7d" or "168h".
    metricsTTL: ${SW_STORAGE_GREPTIMEDB_METRICS_TTL:7d}
    recordsTTL: ${SW_STORAGE_GREPTIMEDB_RECORDS_TTL:3d}
    maxJdbcPoolSize: ${SW_STORAGE_GREPTIMEDB_MAX_JDBC_POOL_SIZE:10}
    metadataQueryMaxSize: ${SW_STORAGE_GREPTIMEDB_QUERY_MAX_SIZE:5000}
```

| Property | Environment variable | Default | Description |
| --- | --- | --- | --- |
| `grpcEndpoints` | `SW_STORAGE_GREPTIMEDB_GRPC_ENDPOINTS` | `127.0.0.1:4001` | GreptimeDB gRPC endpoints for writes, separated by commas. |
| `jdbcEndpoints` | `SW_STORAGE_GREPTIMEDB_JDBC_ENDPOINTS` | `127.0.0.1:4002` | GreptimeDB MySQL endpoints for reads and DDL. Multiple endpoints use Connector/J load balancing and failover. |
| `database` | `SW_STORAGE_GREPTIMEDB_DATABASE` | `skywalking` | Database name. OAP creates it if it does not exist. |
| `user` | `SW_STORAGE_GREPTIMEDB_USER` | `""` | Authentication username. |
| `password` | `SW_STORAGE_GREPTIMEDB_PASSWORD` | `""` | Authentication password. |
| `metricsTTL` | `SW_STORAGE_GREPTIMEDB_METRICS_TTL` | `7d` | TTL for metrics at every downsampling level. |
| `recordsTTL` | `SW_STORAGE_GREPTIMEDB_RECORDS_TTL` | `3d` | Shared TTL for traces, logs, alarms, profiling data, and other records. |
| `maxJdbcPoolSize` | `SW_STORAGE_GREPTIMEDB_MAX_JDBC_POOL_SIZE` | `10` | Maximum JDBC connection pool size. |
| `metadataQueryMaxSize` | `SW_STORAGE_GREPTIMEDB_QUERY_MAX_SIZE` | `5000` | Maximum rows returned by metadata queries for services, instances, and endpoints. |

### Architecture and data model

The plugin uses two GreptimeDB protocols:

- gRPC on port 4001 for asynchronous writes through the [GreptimeDB Java Ingester SDK](https://docs.greptime.com/user-guide/ingest-data/for-iot/grpc-sdks/java).
- The MySQL protocol on port 4002 for queries, DDL, and database bootstrap through JDBC.

SkyWalking models use different GreptimeDB table modes:

| SkyWalking model | GreptimeDB mode | Behavior |
| --- | --- | --- |
| Metrics | `merge_mode='last_row'` | Upsert aggregated time-series data. |
| Records | `append_mode='true'` | Append time-relative records such as traces, logs, alarms, and profiling data. |
| Management | `merge_mode='last_row'` | Upsert current configuration by `id`. |

Searchable trace, log, and alarm tags and Zipkin annotations are stored as normalized rows in additional append-only tables. Raw `key=value` values use skipping indexes, and exact filters use correlated `EXISTS` predicates. Changing the searchable-tag whitelist does not change the table schema.

Current-state metadata is stored as hourly snapshots. Metrics keep one physical version per series and hour through `merge_mode='last_row'`. Management data, including UI templates and continuous-profiling policies, uses `ttl = 'forever'`.

GreptimeDB applies TTL through table options and handles time-based partitioning through the TIME INDEX. The plugin does not create date-partitioned tables or run manual history deletion.

### Cluster deployment and transport security

List each GreptimeDB frontend MySQL endpoint in `jdbcEndpoints`, for example:

```text
frontend-0:4002,frontend-1:4002,frontend-2:4002
```

The plugin uses Connector/J load balancing for database bootstrap and the JDBC connection pool. It fails over when a listed frontend is unavailable. `grpcEndpoints` also accepts a comma-separated list of GreptimeDB gRPC endpoints.

The current plugin configuration exposes username and password authentication but has no TLS or CA settings. Do not send credentials or telemetry over an untrusted network. Use a trusted private network or a local TLS-terminating proxy. A direct TLS connection from OAP has not been validated for this version of the plugin.

GreptimeDB replication, sharding, and storage placement are configured on the GreptimeDB side, not in the OAP plugin. See the [GreptimeDB documentation](https://docs.greptime.com/) for cluster deployment.

### Schema changes and upgrades

OAP creates tables automatically. On later starts, it validates columns, primary keys, indexes, table mode, and TTL against the generated schema. The plugin does not alter or migrate an incompatible table.

Changing `metricsTTL`, `recordsTTL`, or a plugin version that changes the generated schema can make validation fail. Do not drop a production table just to make OAP start: dropping a table deletes its data. Back up the data first, or configure a new database and let OAP create a fresh schema.

### Known limitations

- Log full-text search uses the English analyzer.
- SkyWalking Trace V2 queries are only available with BanyanDB storage.
- Current-state metadata has hourly snapshot granularity. Minute-level historical presence within the same hour is not preserved.
- The plugin has no TLS or CA configuration for gRPC or JDBC connections; direct TLS has not been validated.
- Schema migration is not automatic.
