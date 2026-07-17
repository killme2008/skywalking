# SkyWalking with GreptimeDB

<p align="center">
  <a href="README.md">English</a> | <a href="README_zh.md">简体中文</a>
</p>

[![Build and publish](https://github.com/killme2008/skywalking/actions/workflows/package-greptimedb.yaml/badge.svg)](https://github.com/killme2008/skywalking/actions/workflows/package-greptimedb.yaml)
[![Latest tag](https://img.shields.io/github/v/tag/killme2008/skywalking)](https://github.com/killme2008/skywalking/tags)
[![Container](https://img.shields.io/badge/GHCR-greptimedb--oap-2496ED?logo=docker&logoColor=white)](https://github.com/killme2008/skywalking/pkgs/container/greptimedb-oap)
[![License](https://img.shields.io/github/license/apache/skywalking)](LICENSE)

An unofficial downstream build of [Apache SkyWalking](https://github.com/apache/skywalking) that adds GreptimeDB as an OAP storage backend.

This fork writes telemetry data through GreptimeDB's gRPC API and uses its MySQL-compatible protocol for queries and DDL. The published OAP image includes the GreptimeDB storage plugin and is tested against SkyWalking's storage E2E suites.

This is a community build for testing and evaluation, not an Apache Software Foundation release.

## What is supported

| Area | Scope |
| --- | --- |
| Metrics | SkyWalking metrics ingestion and query, with last-row merge semantics and native TTL. |
| Records | Traces, logs, alarms, Zipkin data, trace profiling, and pprof profiling. Records are append-only with configurable TTL. |
| Search | Exact filters on searchable trace, log, and alarm tags and Zipkin annotations. Log keyword search is supported through `matches_term`. |
| Management data | UI templates and continuous-profiling policies, retained without TTL. |
| Cluster access | Multiple gRPC write endpoints; multiple JDBC frontend endpoints with Connector/J load balancing and failover. |
| Schema lifecycle | Tables are created automatically. Existing tables are validated; incompatible schemas must be dropped and recreated. |

Current limitations:

- Log full-text search uses the English analyzer.
- SkyWalking Trace V2 queries are only available with BanyanDB storage.
- Current-state metadata keeps hourly snapshots, not minute-level history within an hour.

## Try it

The current image is:

```text
ghcr.io/killme2008/greptimedb-oap:11.0.0-greptimedb.1
```

GreptimeDB v1.1.2 or later is required. MySQL Connector/J is also required but is not included in the image. Connector/J is released under [GPLv2 with the Universal FOSS Exception](https://github.com/mysql/mysql-connector-j/blob/release/8.x/LICENSE). Apache classifies GPL dependencies, including most exceptions, as [Category X](https://www.apache.org/legal/resolved.html#category-x) and does not allow them in ASF distributions. This fork follows the same distribution rule.

Download the driver separately and mount it into `/skywalking/ext-libs`. The example assumes a GreptimeDB container named `greptimedb` on the same Docker network:

```bash
docker run --rm \
  --network your-network \
  -p 11800:11800 \
  -p 12800:12800 \
  -v /path/to/mysql-connector-j.jar:/skywalking/ext-libs/mysql-connector-j.jar:ro \
  -e SW_STORAGE=greptimedb \
  -e SW_STORAGE_GREPTIMEDB_GRPC_ENDPOINTS=greptimedb:4001 \
  -e SW_STORAGE_GREPTIMEDB_JDBC_ENDPOINTS=greptimedb:4002 \
  ghcr.io/killme2008/greptimedb-oap:11.0.0-greptimedb.1
```

See the [GreptimeDB storage documentation](docs/en/setup/backend/storages/greptimedb.md) for configuration, deployment, and known limitations.

## Help bring this upstream

I have proposed official GreptimeDB storage support in [apache/skywalking discussion #13722](https://github.com/apache/skywalking/discussions/13722).

If you want to use GreptimeDB with SkyWalking, please add your use case to the discussion. Details such as data volume, retention, deployment model, and required query features are more useful than a simple `+1`.

Please try the image and report implementation or packaging problems in this repository. Use the upstream discussion for product demand and design feedback.

## License

[Apache License 2.0](LICENSE)
