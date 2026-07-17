# SkyWalking GreptimeDB 存储版

<p align="center">
  <a href="README.md">English</a> | <a href="README_zh.md">简体中文</a>
</p>

[![Build and publish](https://github.com/killme2008/skywalking/actions/workflows/package-greptimedb.yaml/badge.svg)](https://github.com/killme2008/skywalking/actions/workflows/package-greptimedb.yaml)
[![Latest tag](https://img.shields.io/github/v/tag/killme2008/skywalking)](https://github.com/killme2008/skywalking/tags)
[![Container](https://img.shields.io/badge/GHCR-greptimedb--oap-2496ED?logo=docker&logoColor=white)](https://github.com/killme2008/skywalking/pkgs/container/greptimedb-oap)
[![License](https://img.shields.io/github/license/apache/skywalking)](LICENSE)

这是 [Apache SkyWalking](https://github.com/apache/skywalking) 的非官方下游版本，为 OAP 增加了 GreptimeDB 存储后端。

`v11.0.0-greptimedb.1` 基于 Apache SkyWalking `11.0.0-SNAPSHOT`，对应上游 commit [`46129f18`](https://github.com/apache/skywalking/commit/46129f18e815829ea14afce9a013bae7d8dfdc66)。

写入通过 GreptimeDB gRPC API 完成，查询和 DDL 使用 MySQL 兼容协议。发布的 OAP 镜像已经包含 GreptimeDB storage plugin，并通过 SkyWalking storage E2E 测试验证。

这是供社区测试和验证的构建，不是 Apache Software Foundation 的正式发行版。

## 支持范围

| 领域 | 当前能力 |
| --- | --- |
| Metrics | 支持 SkyWalking metrics 写入和查询，使用 last-row merge 语义和 GreptimeDB 原生 TTL。 |
| Records | 支持 traces、logs、alarms、events、browser error logs、Zipkin data 和 profiling data。Records 采用 append-only 模式，TTL 可配置。 |
| Search | 支持按 searchable trace、log、alarm tags 和 Zipkin annotations 精确过滤；通过 `matches_term` 查询日志关键词。 |
| Profiling | 支持 trace profiling、async-profiler、eBPF profiling、pprof、JFR data 和 span-attached events。 |
| Management data | 支持 UI templates、runtime rules、network address aliases、service labels 和 continuous-profiling policies，不设置 TTL。 |
| Cluster access | 写入可配置多个 gRPC endpoints；JDBC 可配置多个 frontend endpoints，并通过 Connector/J 提供 load balancing 和 failover。 |
| Schema lifecycle | OAP 启动时自动建表并校验已有表；schema 不兼容时需要删除后重建。 |

当前限制：

- 日志全文检索使用 English analyzer。
- SkyWalking Trace V2 query 目前只支持 BanyanDB storage。
- Current-state metadata 按小时保留快照，不保留同一小时内的分钟级历史。
- Plugin 没有 TLS 或 CA 配置，尚未验证 direct TLS。
- 不支持自动 schema migration。

## 快速试用

当前镜像：

```text
ghcr.io/killme2008/greptimedb-oap:11.0.0-greptimedb.1
```

需要 GreptimeDB v1.1.2 或更高版本。镜像不包含 MySQL Connector/J。Connector/J 使用 [GPLv2 with the Universal FOSS Exception](https://github.com/mysql/mysql-connector-j/blob/release/8.x/LICENSE)；Apache 将 GPL 依赖及大多数例外归为 [Category X](https://www.apache.org/legal/resolved.html#category-x)，不允许打进 ASF 发行物。本 fork 沿用同样的分发边界。

请单独下载 Connector/J，并挂载到 `/skywalking/ext-libs`。下面假设同一 Docker network 中已经有名为 `greptimedb` 的容器：

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

配置、部署方式和已知限制见 [GreptimeDB 存储文档](docs/zh/setup/backend/storages/greptimedb.md)。

## 推进上游支持

我已经在 Apache SkyWalking 社区发起了 [Discussion #13722](https://github.com/apache/skywalking/discussions/13722)，建议在上游正式支持 GreptimeDB storage。

如果你希望在 SkyWalking 中使用 GreptimeDB，请在 Discussion 中说说你的实际场景。数据规模、保留时间、部署方式、依赖的查询能力，这些信息比单纯回复 `+1` 更有价值。

欢迎测试上面的镜像。实现和打包问题请在本仓库反馈；需求和设计建议请发到上游 Discussion。

## License

[Apache License 2.0](LICENSE)
