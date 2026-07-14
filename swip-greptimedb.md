# [DISCUSS] Add GreptimeDB as Storage Option

## Motivation

[GreptimeDB](https://github.com/GreptimeTeam/greptimedb) is an open-source observability database designed to unify the processing of metrics, logs, and traces. It is Apache-2.0 licensed, written in Rust (query engine powered by Apache DataFusion), and supports SQL and PromQL natively. It exposes MySQL and PostgreSQL wire protocols for reads and a gRPC protocol with language SDKs (Java, Go, etc.) for writes.

I looked at how SkyWalking's storage layer works, and think GreptimeDB is a reasonable fit. Here's why:

**Simpler storage lifecycle management.** The JDBC plugin creates day-suffixed tables (`table_20240301`, `table_20240302`, ...) and rotates them for TTL. GreptimeDB handles TTL natively per table (`WITH ('ttl' = '7d')`), so the plugin just creates each table once, and expired data is cleaned up by the storage engine. This eliminates the `IHistoryDeleteDAO` logic entirely — our implementation is a no-op.

**Time-range queries go through partition pruning instead of index scans.** SkyWalking's query DAOs almost always filter by time range first. In the JDBC plugin, this goes through `WHERE time_bucket BETWEEN ? AND ?` with an INVERTED INDEX. In GreptimeDB, we convert `time_bucket` to a `TIMESTAMP TIME INDEX` column (`greptime_ts`), which is the table's partition key. The storage engine skips irrelevant partitions before touching any index. A concrete example: `TraceQueryDAO.queryBasicTraces()` filters by `greptime_ts >= ? AND greptime_ts <= ?` — this hits partition pruning directly rather than scanning an index over all partitions.

**Upsert without UPDATE.** SkyWalking metrics are aggregated in the OAP's Java layer (`combine()` + `calculate()`), then the full row is written to storage. The JDBC plugin uses `INSERT ... ON DUPLICATE KEY UPDATE` or equivalent. GreptimeDB's `merge_mode=last_row` achieves the same thing with a plain INSERT — same primary key and timestamp → the later write wins. No UPDATE statement needed.

**Append-only workloads skip deduplication.** Records (traces, logs, alarms) are write-once. GreptimeDB's `append_mode=true` tells the storage engine to skip deduplication checks on these tables.

**Object storage as primary storage layer.** GreptimeDB can use S3, Azure Blob Storage, etc. as its storage backend with disaggregated compute/storage. I don't have SkyWalking-specific cost numbers to share, but the architecture means users in cloud environments can scale storage independently from compute. This is documented in detail at [GreptimeDB Storage Location](https://docs.greptime.com/user-guide/concepts/storage-location).

**Disclosure**: I'm from the GreptimeDB community. We'd like to see GreptimeDB become a supported storage option for SkyWalking, and are willing to maintain this plugin long-term.

I have a working implementation ([PR](https://github.com/killme2008/skywalking/pull/1)) with all DAO interfaces implemented, unit tests, and E2E tests passing against the shared `storage-cases.yaml`. Happy to run performance benchmarks if the community is interested — SkyWalking's `benchmarks/` framework and `data-generator` tool could be extended to cover storage backend comparisons.

## Architecture Graph

No architecture-level change to OAP core. This is a new `storage-greptimedb-plugin` module under `server-storage-plugin/`, following the same patterns as existing storage plugins.

```
                      ┌─────────────────────┐
                      │     OAP Server      │
                      └──────┬────┬─────────┘
                  Write path │    │ Read path
                  (gRPC SDK) │    │ (MySQL protocol)
                             │    │
                      ┌──────▼────▼─────────┐
                      │     GreptimeDB      │
                      │  :4001 gRPC         │
                      │  :4002 MySQL        │
                      └─────────────────────┘
```

The plugin uses a dual-protocol approach:
- **Write path**: GreptimeDB's native gRPC ingester SDK (`io.greptime:ingester-all`) for batch writes. The SDK supports async streaming and compression over a persistent gRPC connection, which is a better fit for SkyWalking's write-heavy workload than JDBC INSERT statements.
- **Read path**: Standard JDBC over GreptimeDB's MySQL-compatible protocol, using HikariCP connection pooling. This keeps the query DAO implementation close to what the JDBC plugin does — plain SQL with PreparedStatement.

## Proposed Changes

### New module

[`oap-server/server-storage-plugin/storage-greptimedb-plugin`](https://github.com/killme2008/skywalking/tree/feature/greptimedb-plugin/oap-server/server-storage-plugin/storage-greptimedb-plugin)

### Why not reuse the existing JDBC plugin?

This was the first thing I tried. The JDBC plugin's `TableHelper` uses static methods (`getTable()`, `getLatestTableForWrite()`, `generateId()`) that are called directly from all DAO implementations. These can't be overridden by subclassing. More fundamentally, the day-suffixed table rotation, the `INSERT ON DUPLICATE KEY UPDATE` pattern, and the separate tag tables are all baked into the JDBC plugin's architecture, and all three are things we specifically want to avoid for GreptimeDB.

The BanyanDB plugin took the same approach — a standalone plugin implementing all DAO interfaces from scratch. Our plugin follows that pattern (49 source files + 7 test files, comparable to BanyanDB's 60 + 2).

### Table mapping

SkyWalking data models are mapped to GreptimeDB tables with the following modes:

| Data Type | GreptimeDB Mode | TTL Default | Examples |
|-----------|-----------------|-------------|----------|
| Metrics | `merge_mode=last_row` | 7d | `service_resp_time`, `service_cpm` |
| Records (traces, logs, alarms) | `append_mode=true` | 3d | `segment`, `log`, `alarm_record` |
| Management / NoneStream | `merge_mode=last_row` | no expiry | `ui_template`, `service_traffic` |

Each table has a `greptime_ts TIMESTAMP TIME INDEX` column derived from the model's time bucket, enabling partition pruning on time-range queries.

### Index strategy

GreptimeDB supports several index types. Here's how they map to SkyWalking's query patterns:

**INVERTED INDEX** — for low-to-medium cardinality columns that appear in WHERE equality/range filters.

Example: `AlarmQueryDAO.getAlarm()` filters by `scope`, `layer`, `start_time`. These columns get INVERTED INDEX so the query doesn't scan the full table after partition pruning.

**SKIPPING INDEX** (bloom filter) — for high-cardinality columns used in exact-match lookups. Applies to `trace_id`, `segment_id`, and `unique_id`.

Example: `TraceQueryDAO.queryByTraceId()` does `WHERE trace_id = ?`. A bloom filter can quickly rule out SST files that don't contain the target trace, without maintaining a full inverted index over millions of unique trace IDs.

**FULLTEXT INDEX** — for long text columns like log content. Created with `FULLTEXT INDEX WITH(analyzer = 'English', case_sensitive = 'false')` on string columns longer than 16383 characters.

`LogQueryDAO` wires `keywordsOfContent` / `excludingKeywordsOfContent` to `matches_term(lower(content), lower(?))` over this FULLTEXT index: exact word-level matching, made case-insensitive by applying `lower()` to both the column and the term (the index's `case_sensitive` option only affects the `matches()` query path, not `matches_term`). `supportQueryLogsByKeywords()` returns true.

**Searchable tags as indexed columns** — the JDBC plugin creates separate tag tables (`segment_tag`, `log_tag`, `alarm_record_tag`) and uses INNER JOIN for tag filtering. Instead of a JSON blob (which has no index, so `json_path_match` degrades to a row-by-row scan), each *searchable* tag key (from the `searchableTracesTags` / `searchableLogsTags` / `searchableAlarmTags` whitelists) is promoted to its own indexed column, so tag filtering becomes `` WHERE `http.method` = ? `` and is pushed down:

```sql
-- Find traces where http.method = GET
SELECT * FROM segment
WHERE greptime_ts >= ? AND greptime_ts <= ?
  AND service_id = ?
  AND `http.method` = 'GET'
```

- The column name is the tag key verbatim — GreptimeDB accepts dotted, back-quoted identifiers, so no `.`→`_` mapping and no collision handling is needed.
- The `primaryKeyTags` config (a subset of the whitelist, default `http.method,status_code`) marks the high-frequency keys that join the PRIMARY KEY (low cardinality → row-group pruning). The remaining whitelist tags become field columns with an INVERTED INDEX added at table-creation time.
- The whitelist is watched at runtime. Keys added after table creation are appended via `ALTER TABLE ADD COLUMN` as plain fields — `ALTER` cannot attach an INVERTED INDEX, so such keys are queryable but unindexed until the table is rebuilt.
- Writes split each `k=v` in `tags`; whitelist keys populate their columns, everything else stays in `dataBinary` (full payload, unsearchable). Reads validate the tag key against the whitelist first (a non-searchable key forces an empty result), mirroring the JDBC/ES plugins.
- Scope: trace (`segment`), log, alarm. Zipkin (annotation → `query` FULLTEXT column) and tag auto-completion (its own `*_tag_autocomplete` table) are unaffected.

### DAO implementations

All 40+ DAO interfaces required by `StorageModule` are implemented, covering:
- Metrics, Aggregation, Records, TopN
- Traces, Logs, Browser Logs, Events
- Alarms (with recovery records)
- Topology (service, endpoint, process)
- Metadata (services, instances, endpoints)
- Profiling (trace, eBPF, continuous, async profiler, pprof)
- UI management (templates, menus)
- Zipkin compatibility
- Tag auto-completion, Span attached events, Hierarchy

### Testing

A full E2E test case at `test/e2e-v2/cases/storage/greptimedb/` using the shared `storage-cases.yaml` verification suite. Tested against GreptimeDB v1.0.0-rc.1. Unit tests cover type conversion, DDL generation, schema registry, and query helper logic (94 tests total).

For step-by-step instructions on running unit tests, E2E tests, and a quick-start demo, see [`TESTING.md`](https://github.com/killme2008/skywalking/blob/feature/greptimedb-plugin/oap-server/server-storage-plugin/storage-greptimedb-plugin/TESTING.md).

## Imported Dependencies libs and their licenses

| Dependency | Version | License |
|-----------|---------|---------|
| `io.greptime:ingester-all` | 0.15.0 | Apache-2.0 |
| `com.mysql:mysql-connector-j` | 9.2.0 | GPL-2.0 with [Universal FOSS Exception](https://oss.oracle.com/licenses/universal-foss-exception/) |

Notes:
- The GreptimeDB Java SDK's transitive gRPC dependencies are excluded in favor of SkyWalking's existing gRPC version (`grpc-netty-shaded`, `grpc-protobuf`, `grpc-stub`).
- `mysql-connector-j` is used to connect to GreptimeDB's MySQL-compatible protocol port. The Universal FOSS Exception explicitly permits use in Apache-2.0 licensed projects. That said, the existing JDBC storage plugin takes a different approach — it doesn't bundle any JDBC driver but relies on users providing one at runtime. If bundling `mysql-connector-j` is a concern, we can adopt the same approach and document it as a deployment requirement. Happy to discuss what the community prefers here.

## Compatibility

No breaking changes.

- **Protocol**: No changes to Query Protocol or Data Collect Protocol.
- **Configuration**: New `storage.selector=greptimedb` option. No impact on existing storage configurations.
- **Other storage plugins**: No modifications to any existing plugin.

## General usage docs

### GreptimeDB setup

GreptimeDB can run in standalone or cluster mode. For a quick start:

```bash
docker run -p 4000:4000 -p 4001:4001 -p 4002:4002 \
  greptime/greptimedb:v1.0.0-rc.1 standalone start \
  --http-addr=0.0.0.0:4000 \
  --rpc-bind-addr=0.0.0.0:4001 \
  --mysql-addr=0.0.0.0:4002
```

### OAP configuration

In `application.yml`:

```yaml
storage:
  selector: ${SW_STORAGE:greptimedb}
  greptimedb:
    grpcEndpoints: ${SW_STORAGE_GREPTIMEDB_GRPC_ENDPOINTS:127.0.0.1:4001}
    jdbcHost: ${SW_STORAGE_GREPTIMEDB_JDBC_HOST:127.0.0.1}
    jdbcPort: ${SW_STORAGE_GREPTIMEDB_JDBC_PORT:4002}
    database: ${SW_STORAGE_GREPTIMEDB_DATABASE:skywalking}
    user: ${SW_STORAGE_GREPTIMEDB_USER:}
    password: ${SW_STORAGE_GREPTIMEDB_PASSWORD:}
    metricsTTL: ${SW_STORAGE_GREPTIMEDB_METRICS_TTL:7d}
    recordsTTL: ${SW_STORAGE_GREPTIMEDB_RECORDS_TTL:3d}
```

Tables are created automatically on OAP startup.

## References

GreptimeDB documentation for concepts mentioned in this proposal:

- [Data Model](https://docs.greptime.com/user-guide/concepts/data-model) — TIME INDEX, Tag/Field column types, table schema
- [Table Design Best Practices](https://docs.greptime.com/user-guide/deployments-administration/performance-tuning/design-table/) — primary key selection, merge_mode vs append_mode, cardinality guidelines
- [Data Index](https://docs.greptime.com/user-guide/manage-data/data-index) — INVERTED INDEX, SKIPPING INDEX, FULLTEXT INDEX
- [Storage Location](https://docs.greptime.com/user-guide/concepts/storage-location) — local disk, S3, Azure Blob Storage, etc.
- [Java Ingester SDK](https://docs.greptime.com/user-guide/ingest-data/for-iot/grpc-sdks/java) — gRPC write path, StreamWriter API
- [MySQL Protocol](https://docs.greptime.com/user-guide/protocols/mysql) — wire protocol compatibility used by the read path
- [JSON Functions](https://docs.greptime.com/reference/sql/functions/json/) — JSON column operations; `json_path_match()` used for tag filtering ([added in v0.10.2](https://github.com/GreptimeTeam/greptimedb/pull/4864))
- [TTL (Time-To-Live)](https://docs.greptime.com/user-guide/manage-data/overview#manage-data-retention-with-ttl-policies) — per-table data expiration
