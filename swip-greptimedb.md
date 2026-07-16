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
- **Read path**: Standard JDBC over GreptimeDB's MySQL-compatible protocol, using HikariCP connection pooling. Multiple frontend endpoints use Connector/J's load-balancing connection mode for failover. This keeps the query DAO implementation close to what the JDBC plugin does — plain SQL with PreparedStatement.

## Proposed Changes

### New module

[`oap-server/server-storage-plugin/storage-greptimedb-plugin`](https://github.com/killme2008/skywalking/tree/feature/greptimedb-plugin/oap-server/server-storage-plugin/storage-greptimedb-plugin)

### Why not reuse the existing JDBC plugin?

This was the first thing I tried. The JDBC plugin's `TableHelper` uses static methods (`getTable()`, `getLatestTableForWrite()`, `generateId()`) that are called directly from all DAO implementations. These can't be overridden by subclassing. More fundamentally, its day-suffixed table rotation and `INSERT ON DUPLICATE KEY UPDATE` write path do not map cleanly to GreptimeDB's native TTL, TIME INDEX, merge modes, and gRPC ingestion.

The BanyanDB plugin took the same approach — a standalone plugin implementing all DAO interfaces from scratch. Our plugin follows that pattern.

### Table mapping

SkyWalking data models are mapped to GreptimeDB tables with the following modes:

| Data Type | GreptimeDB Mode | TTL Default | Examples |
|-----------|-----------------|-------------|----------|
| Metrics | `merge_mode=last_row` | 7d | `service_resp_time`, `service_traffic` |
| Records (traces, logs, alarms, profiling tasks) | `append_mode=true` | 3d | `segment`, `log`, `pprof_task` |
| Management | `merge_mode=last_row` | no expiry | `ui_template`, `continuous_profiling_policy` |

Each table has a `greptime_ts TIMESTAMP TIME INDEX` column. Regular time-series rows, including NoneStream profiling tasks, derive it from the model's time bucket. Current-state metadata metrics marked with BanyanDB's `IndexMode` truncate it to the start of the hour, so `merge_mode=last_row` keeps one physical version per series and hour. Only non-time-series management rows use a fixed timestamp so updates overwrite the existing row. Time-range queries use `greptime_ts` for partition and row-group pruning.

DDL generation, gRPC row encoding, and startup validation use the same generated schema contract. If an existing table differs in columns, types, semantic types, primary keys, indexes, table mode, or TTL, startup fails with an instruction to drop and recreate the table. The plugin does not reconcile an incompatible schema with `ALTER TABLE`.

### Index strategy

GreptimeDB supports several index types. The plugin uses an explicit `(model, column)` allow-list: a secondary index is created only when a query DAO has a matching predicate. Primary keys are reserved for series identity and deduplication rather than used as a general query-index mechanism.

**INVERTED INDEX** — for low-to-medium cardinality columns used in equality filters, including alarm dimensions, event dimensions, and selected Zipkin dimensions.

Example: alarm queries filter by `scope`, `layer`, and `rule_name`. These dimensions get inverted indexes so the query does not scan every row left after time pruning.

**SKIPPING INDEX** (bloom filter) — for high-cardinality columns used in exact-match lookups. Examples include `trace_id`, `segment_id`, `task_id`, and model-specific entity identifiers.

Example: `TraceQueryDAO.queryByTraceId()` does `WHERE trace_id = ?`. A bloom filter can quickly rule out SST files that don't contain the target trace, without maintaining a full inverted index over millions of unique trace IDs.

Range-filtered columns such as duration and latency do not get skipping indexes because GreptimeDB's bloom-filter skipping index serves equality predicates. These queries rely on the time index and row-group min/max statistics.

**FULLTEXT INDEX** — explicitly enabled on log content with `FULLTEXT INDEX WITH(analyzer = 'English', case_sensitive = 'false')`.

`LogQueryDAO` wires `keywordsOfContent` / `excludingKeywordsOfContent` to `matches_term(lower(content), lower(?))` over this FULLTEXT index: exact word-level matching, made case-insensitive by applying `lower()` to both the column and the term (the index's `case_sensitive` option only affects the `matches()` query path, not `matches_term`). `supportQueryLogsByKeywords()` returns true.

**Searchable tags as normalized rows** — list-valued fields are stored in append-only additional tables such as `segment_tag`, `log_tag`, `alarm_record_tag`, and `zipkin_query`. Each list item becomes one row containing the parent `id`, the exact raw value, and the same `greptime_ts` as the main row. The value column has a skipping index; the additional table has no primary key, avoiding high-cardinality deduplication work.

```sql
-- Find traces where http.method = GET
SELECT main.* FROM segment main
WHERE main.greptime_ts >= ? AND main.greptime_ts <= ?
  AND EXISTS (
    SELECT 1 FROM segment_tag tag
    WHERE tag.id = main.id
      AND tag.greptime_ts = main.greptime_ts
      AND tag.tags = 'http.method=GET'
  )
```

- Trace, log, and alarm values are stored as exact `key=value` strings. One correlated `EXISTS` predicate is emitted for every requested tag, preserving logical-AND and multi-value semantics without multiplying the main result set.
- Searchable-tag whitelists validate which keys may be queried; they do not affect table schema. Whitelist changes therefore require no column addition or table rebuild.
- Zipkin annotation queries use the same normalized layout and preserve the distinction between key-only and exact `key=value` searches.
- Tag auto-completion continues to use its own storage model.

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

A full E2E test case at `test/e2e-v2/cases/storage/greptimedb/` uses the shared `storage-cases.yaml` verification suite. The module also has Testcontainers integration tests for schema validation, merge and append modes, normalized multi-value tags, Zipkin annotation matching, metadata snapshots, metrics reads, and FULLTEXT log queries. Both suites run against GreptimeDB v1.1.2. Unit tests cover conversion, schema generation, index policy, batch grouping, and query construction.

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
  greptime/greptimedb:v1.1.2 standalone start \
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
    jdbcEndpoints: ${SW_STORAGE_GREPTIMEDB_JDBC_ENDPOINTS:127.0.0.1:4002}
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
- [TTL (Time-To-Live)](https://docs.greptime.com/user-guide/manage-data/overview#manage-data-retention-with-ttl-policies) — per-table data expiration
