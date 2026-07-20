# SkyWalking with GreptimeDB

<p align="center">
  <a href="README.md">English</a> | <a href="README_zh.md">简体中文</a>
</p>

[![Build and publish](https://github.com/killme2008/skywalking/actions/workflows/package-greptimedb.yaml/badge.svg)](https://github.com/killme2008/skywalking/actions/workflows/package-greptimedb.yaml)
[![Latest tag](https://img.shields.io/github/v/tag/killme2008/skywalking)](https://github.com/killme2008/skywalking/tags)
[![Container](https://img.shields.io/badge/GHCR-greptimedb--oap-2496ED?logo=docker&logoColor=white)](https://github.com/killme2008/skywalking/pkgs/container/greptimedb-oap)
[![License](https://img.shields.io/github/license/apache/skywalking)](LICENSE)

An unofficial downstream build of [Apache SkyWalking](https://github.com/apache/skywalking) that adds GreptimeDB as an OAP storage backend.

Release `v11.0.0-greptimedb.3` is based on Apache SkyWalking `11.0.0-SNAPSHOT` at upstream commit [`46129f18`](https://github.com/apache/skywalking/commit/46129f18e815829ea14afce9a013bae7d8dfdc66).

This fork writes telemetry data through GreptimeDB's gRPC API and uses its MySQL-compatible protocol for queries and DDL. The published OAP image includes the GreptimeDB storage plugin and is tested against SkyWalking's storage E2E suites.

This is a community build for testing and evaluation, not an Apache Software Foundation release.

## What is supported

| Area | Scope |
| --- | --- |
| Metrics | SkyWalking metrics ingestion and query, with last-row merge semantics and native TTL. |
| Records | Traces, logs, alarms, events, browser error logs, Zipkin data, and profiling data. Records are append-only with configurable TTL. |
| Search | Exact filters on searchable trace, log, and alarm tags and Zipkin annotations. Log keyword search is supported through `matches_term`. |
| Profiling | Trace profiling, async-profiler, eBPF profiling, pprof, JFR data, and span-attached events. |
| Management data | UI templates, runtime rules, network address aliases, service labels, and continuous-profiling policies, retained without TTL. |
| Cluster access | Multiple gRPC write endpoints; multiple JDBC frontend endpoints with Connector/J load balancing and failover. |
| Schema lifecycle | Tables are created automatically. Existing tables are validated; incompatible schemas must be dropped and recreated. |

Current limitations:

- Log full-text search uses the English analyzer.
- SkyWalking Trace V2 queries are only available with BanyanDB storage.
- Current-state metadata keeps hourly snapshots, not minute-level history within an hour.
- The plugin has no TLS or CA configuration; direct TLS has not been validated.
- Schema migration is not automatic.

## Try it

The current release provides a binary distribution and a multi-architecture container image:

- [Binary distribution](https://github.com/killme2008/skywalking/releases/download/v11.0.0-greptimedb.3/apache-skywalking-apm-11.0.0-greptimedb.3-bin.tar.gz)
- [SHA-512 checksum](https://github.com/killme2008/skywalking/releases/download/v11.0.0-greptimedb.3/apache-skywalking-apm-11.0.0-greptimedb.3-bin.tar.gz.sha512)

```text
ghcr.io/killme2008/greptimedb-oap:11.0.0-greptimedb.3
```

The plugin is tested against GreptimeDB v0.15.5 and v1.1.2 in CI, with an additional manual smoke test against v0.17.2. MySQL Connector/J is also required but is not included in the image. Connector/J is released under [GPLv2 with the Universal FOSS Exception](https://github.com/mysql/mysql-connector-j/blob/release/8.x/LICENSE). Apache classifies GPL dependencies, including most exceptions, as [Category X](https://www.apache.org/legal/resolved.html#category-x) and does not allow them in ASF distributions. This fork follows the same distribution rule.

The [Docker quick start](docs/en/setup/backend/storages/greptimedb.md#docker-quick-start) covers the complete setup. If GreptimeDB is already running as `greptimedb` on the `skywalking-greptimedb` Docker network, download the driver separately, mount it into `/skywalking/ext-libs`, and start OAP with:

```bash
docker run -d \
  --name skywalking-oap \
  --network skywalking-greptimedb \
  -p 11800:11800 \
  -p 12800:12800 \
  -p 9411:9411 \
  -v /path/to/mysql-connector-j.jar:/skywalking/ext-libs/mysql-connector-j.jar:ro \
  -e SW_STORAGE=greptimedb \
  -e SW_STORAGE_GREPTIMEDB_GRPC_ENDPOINTS=greptimedb:4001 \
  -e SW_STORAGE_GREPTIMEDB_JDBC_ENDPOINTS=greptimedb:4002 \
  -e SW_STORAGE_GREPTIMEDB_DATABASE=skywalking \
  -e SW_HEALTH_CHECKER=default \
  -e SW_RECEIVER_ZIPKIN=default \
  -e SW_QUERY_ZIPKIN=default \
  -e "JAVA_OPTS=-Xms1g -Xmx1g" \
  ghcr.io/killme2008/greptimedb-oap:11.0.0-greptimedb.3
```

Continue with [Start Horizon UI](docs/en/setup/backend/storages/greptimedb.md#start-horizon-ui), then connect an instrumented service to port `11800`.

After traffic reaches OAP, Horizon should show service metrics and complete traces across services and database calls:

![Services dashboard](docs/en/setup/backend/images/greptimedb/services-dashboard.png)

![Trace detail](docs/en/setup/backend/images/greptimedb/trace-detail.png)

See the [GreptimeDB storage documentation](docs/en/setup/backend/storages/greptimedb.md) for configuration, deployment, and known limitations.

## Help bring this upstream

I have proposed official GreptimeDB storage support in [apache/skywalking discussion #13722](https://github.com/apache/skywalking/discussions/13722).

If you want to use GreptimeDB with SkyWalking, please add your use case to the discussion. Details such as data volume, retention, deployment model, and required query features are more useful than a simple `+1`.

Please try the image and report implementation or packaging problems in this repository. Use the upstream discussion for product demand and design feedback.

## License

[Apache License 2.0](LICENSE)
