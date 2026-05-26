# IPFAR 桥节点配置参数文档

## 概述

桥节点通过 JSON 配置文件控制所有行为。默认路径为运行目录下的 `config.json`。

首次运行或配置文件不存在时，程序自动从嵌入资源中释放默认 `config.json`。所有参数均为可选（`omitempty`），未设置的字段将使用代码内默认值。

## 快速启动

```bash
ipfar init          # 生成默认 config.json
ipfar serve         # 启动桥节点（默认读取 config.json）
ipfar serve --config /path/to/config.json  # 指定配置文件
ipfar config show   # 查看当前配置
```

## 完整配置示例

```json
{
  "language": "en_US",
  "log_level": "info",
  "log_file": "logs/ipfar.log",
  "log_to_console": false,
  "log_format": "text",

  "verify_pow": true,
  "verify_index": true,
  "verify_reference_chain": true,
  "verify_integrity": false,
  "security_preset": "light",

  "gateway_list": ["https://arweave.net"],
  "gateway_allow_local": false,
  "gateway_health_check": true,
  "gateway_health_check_secs": 60,
  "gateway_timeout_secs": 30,
  "gateway_max_retries": 3,

  "discovery_mode": "graphql-scan",
  "min_block_height": 1919626,
  "max_block_height": 0,
  "poll_interval_secs": 120,
  "scan_batch_size": 100,
  "scan_query_delay_ms": 2000,

  "dht": {
    "enabled": true,
    "mode": "server",
    "bootstrap_peers": [],
    "reprovide_interval": "12h",
    "provide_concurrency": 4,
    "retry_max_attempts": 5,
    "listen_addresses": [
      "/ip4/0.0.0.0/tcp/4001",
      "/ip4/0.0.0.0/udp/4001/quic-v1",
      "/ip4/0.0.0.0/udp/4001/quic-v1/webtransport",
      "/ip6/::/tcp/4001",
      "/ip6/::/udp/4001/quic-v1",
      "/ip6/::/udp/4001/quic-v1/webtransport",
      "/ip4/0.0.0.0/udp/4001/webrtc-direct",
      "/ip6/::/udp/4001/webrtc-direct"
    ]
  }
}
```

> **注意：** 以上为 `config.json` 中可配置的全部参数。实际运行时还有部分行为参数通过 CLI 标志控制（见 [§8 CLI 标志参考](#8-cli-标志参考)）。

---

## 参数参考

### 1. 基础设置

| 参数 | 类型 | 默认值 | 可选值 | 说明 |
|------|------|--------|--------|------|
| `language` | `string` | `"en_US"` | `"en_US"`, `"zh_CN"`, … | 界面语言。首次启动时自动检测系统语言。支持的语言由 `lang/` 目录下的 `.po` 文件决定。 |
| `log_level` | `string` | `"info"` | `"debug"`, `"info"`, `"warn"`, `"error"` | 日志输出级别。`debug` 输出所有调试信息；`error` 仅输出错误。 |
| `log_file` | `string` | `"logs/ipfar.log"` | 任意有效文件路径 | 日志文件写入路径。为空时不写文件。 |
| `log_to_console` | `bool` | `false` | `true` / `false` | 是否同时输出日志到控制台（stderr）。 |
| `log_format` | `string` | `"text"` | `"text"`, `"json"` | 日志输出格式。`json` 适用于日志采集系统；`text` 适合人工阅读。 |

---

### 2. 验证选项（规范 §4.1）

四个独立开关控制验证管道的各个步骤。若同时设置了 `security_preset`（见 [§2.1](#21-安全预设-security_preset)），预设将覆盖这些单独选项。

| 参数 | 类型 | 默认值 | 可选值 | 说明 |
|------|------|--------|--------|------|
| `verify_pow` | `bool` | `true` | `true` / `false` | 是否验证 Arweave 区块的 PoW（工作量证明）。关闭可提升性能，但降低安全性。 |
| `verify_index` | `bool` | `true` | `true` / `false` | 是否验证 Index 数据完整性。**规范 §3.4 要求始终开启**；即使设为 `false`，代码层也会强制为 `true`。 |
| `verify_reference_chain` | `bool` | `true` | `true` / `false` | 是否验证元数据引用的父交易链完整性。涉及跨交易递归验证。 |
| `verify_integrity` | `bool` | `false` | `true` / `false` | 是否验证数据块完整性（逐块哈希校验）。仅在完整下载路径（路径 B）下生效。计算开销较高。 |

#### 2.1 安全预设 (`security_preset`)

`security_preset` 提供一键设置验证强度的便捷方式。设置后，四个独立开关的显式值将被忽略。

| 预设值 | PoW | Index | 引用链 | 完整性 | 说明 |
|--------|-----|-------|--------|--------|------|
| `"strict"` | ✅ | ✅ | ✅ | ✅ | 最高安全性。启用全部验证，包括完整性。适合对数据可靠性要求极高的场景。 |
| `"balanced"` | ✅ | ✅ | ❌ | ✅ | 平衡模式。关闭引用链验证以降低网络开销，保留完整性验证。 |
| `"light"` **(默认)** | ✅ | ✅ | ✅ | ❌ | 轻量模式。跳过完整性验证（最昂贵的步骤），适合大多数场景。 |
| `"trusted"` | ❌ | ✅ | ❌ | ❌ | 信任模式。仅保留 Index 验证（规范强制要求）。适合内网或信任环境。 |

> **关联规范章节：** `ipfar-specs/V1/` §4.1（验证管道）

---

### 3. 网关配置（规范 §3）

桥节点通过 Arweave HTTP 网关获取区块数据和元数据。

| 参数 | 类型 | 默认值 | 可选值 | 说明 |
|------|------|--------|--------|------|
| `gateway_list` | `[]string` | `["https://arweave.net"]` | 任意 Arweave 网关 URL 列表 | Arweave 网关地址列表。按顺序尝试，第一个可用者被选中。支持自建网关。 |
| `gateway_allow_local` | `bool` | `false` | `true` / `false` | 是否允许使用本地网关（如 `http://localhost:1984`）。生产环境建议关闭。 |
| `gateway_health_check` | `bool` | `true` | `true` / `false` | 是否在启动时对网关列表执行健康检查。不可用的网关会被自动跳过。 |
| `gateway_health_check_secs` | `int` | `60` | ≥0（秒） | 定期健康检查间隔。设为 0 表示仅启动时检查一次。 |
| `gateway_timeout_secs` | `int` | `30` | ≥1（秒） | 单次网关 HTTP 请求超时时间。网络条件差的环境可适当增大。 |
| `gateway_max_retries` | `int` | `3` | ≥0 | 网关请求失败后的最大重试次数。0 表示不重试。 |

> **关联规范章节：** `ipfar-specs/V1/` §3（网关接口）

---

### 4. 发现配置（规范 §3.1 / §3.2）

发现模块负责从 Arweave 区块链中定位 IPFAR 元数据交易。

| 参数 | 类型 | 默认值 | 可选值 | 说明 |
|------|------|--------|--------|------|
| `discovery_mode` | `string` | `"graphql-scan"` | `"sampling"`, `"graphql"`, `"graphql-scan"` | 区块发现策略（详见下方）。 |
| `min_block_height` | `uint64` | `1919626` | ≥0 | 扫描起始区块高度。默认为 IPFAR 创世区块。 |
| `max_block_height` | `uint64` | `0` | 0 或 ≥ `min_block_height` | 扫描终止区块高度。`0` 表示动态跟随最新高度。 |
| `poll_interval_secs` | `int` | `120` | ≥1（秒） | 轮询间隔。在 `sampling` 和 `graphql` 模式下，控制每次发现尝试的间隔。 |
| `scan_batch_size` | `int` | `100` | ≥1 | 扫描批次大小。`graphql-scan` 模式下，每次 GraphQL 查询获取的区块数量。 |
| `scan_query_delay_ms` | `int` | `2000` | ≥0（毫秒） | GraphQL 查询间延迟。`graphql-scan` 模式下，批次间的等待时间，用于避免网关限流。 |

#### 发现模式详解

| 模式 | 说明 | 适用场景 |
|------|------|----------|
| `graphql-scan` | **（默认）** 使用 Arweave GraphQL 接口顺序扫描区块。从 `min_block_height` 开始，按批次查询，自动排除已处理的区块。同时启动区块监听器跟踪新区块。 | 通用场景，覆盖面最广。 |
| `graphql` | 单块 GraphQL 查询。每次仅查询最新区块，配合轮询间隔使用。 | 仅关注最新数据的轻量场景。 |
| `sampling` | 随机游走抽样。在高区块范围内随机选择区块进行 GraphQL 检查。 | 快速探测、统计抽样。 |

> **关联规范章节：** `ipfar-specs/V1/` §3.1（发现策略）、§3.2（GraphQL 接口）

---

### 5. DHT 内容发布（规范 §5 / P3-1）

DHT（Kademlia）模块将已验证的 CID 发布到 IPFS 网络，使其他节点可通过 CID 检索数据。

所有 DHT 参数嵌套在 `"dht"` 对象下。

| 参数 | 类型 | 默认值 | 可选值 | 说明 |
|------|------|--------|--------|------|
| `dht.enabled` | `bool` | `true` | `true` / `false` | 是否启用 DHT 内容发布。关闭后桥节点不参与 IPFS DHT 网络。 |
| `dht.mode` | `string` | `"server"` | `"server"`, `"client"` | DHT 运行模式。`server` 为其他节点提供路由服务；`client` 仅查询和发布。 |
| `dht.bootstrap_peers` | `[]string` | `[]` | Multiaddr 字符串列表 | 引导节点地址列表。为空时使用 IPFS 默认引导节点。格式如 `"/ip4/1.2.3.4/tcp/4001/p2p/12D3..."` |
| `dht.reprovide_interval` | `string` | `"12h"` | Go duration 字符串 | 重新提供间隔。如 `"12h"`、`"30m"`、`"1h30m"`。CID 记录在 DHT 中有过期时间，定期 re-provide 可防止路由丢失。 |
| `dht.provide_concurrency` | `int` | `4` | ≥1 | Provide 操作并发数。影响向 DHT 发布 CID 的速度。 |
| `dht.retry_max_attempts` | `int` | `5` | ≥0 | Provide 失败后的最大重试次数。`0` 表示无限重试。 |
| `dht.listen_addresses` | `[]string` | 见下方默认值 | Multiaddr 字符串列表 | libp2p host 监听地址。配置后将自动创建 host 和 DHT provider。为空时不自动启动 host（需外部注入）。 |

**默认监听地址：**

```json
[
  "/ip4/0.0.0.0/tcp/4001",
  "/ip4/0.0.0.0/udp/4001/quic-v1",
  "/ip4/0.0.0.0/udp/4001/quic-v1/webtransport",
  "/ip6/::/tcp/4001",
  "/ip6/::/udp/4001/quic-v1",
  "/ip6/::/udp/4001/quic-v1/webtransport",
  "/ip4/0.0.0.0/udp/4001/webrtc-direct",
  "/ip6/::/udp/4001/webrtc-direct"
]
```

> **关联规范章节：** `ipfar-specs/V1/` §5（内容发布）、`P3-1-DHT内容发布.md`

---

### 6. Bitswap 配置（规范 §4）

Bitswap 是 IPFS 的数据交换协议。桥节点的 Bitswap 服务端允许 IPFS 节点按需拉取已验证的数据块。

> **当前状态：** Bitswap 参数通过 `bridge.ServiceConfig` 控制，尚未暴露为 `config.json` 字段。以下为代码默认值。

| 参数 | 类型 | 默认值 | 可选值 | 说明 |
|------|------|--------|--------|------|
| `bitswap_enabled` | `bool` | `true` | `true` / `false` | 是否启用 Bitswap 按需拉取服务。关闭后 IPFS 节点无法从本桥节点拉取数据块。 |
| `bitswap_port` | `int` | `4001` | 1–65535 | Bitswap TCP 监听端口。若端口被占用，自动递增尝试最多 5 次。 |
| `cache_size` | `int64` | `1073741824` (1 GiB) | ≥0（字节） | Bitswap 数据块缓存上限。超过后按 LRU 淘汰旧块。 |

> **关联规范章节：** `ipfar-specs/V1/` §4（Bitswap 集成）

---

### 7. libp2p 网络配置

libp2p host 是 DHT 模块的底层网络栈。以下参数通过 `HostConfig` 结构体控制，**目前未在 `config.json` 中暴露**，使用代码默认值。高级用户可通过修改 `dht.listen_addresses` 间接影响 host 行为。

| 参数 | 类型 | 默认值 | 可选值 | 说明 |
|------|------|--------|--------|------|
| `enable_relay` | `bool` | `true` | `true` / `false` | 启用 Circuit Relay v2 中继协议。允许 NAT 后的节点通过中继服务器通信。 |
| `enable_auto_nat` | `bool` | `true` | `true` / `false` | 启用 AutoNAT v1。自动检测节点是否位于 NAT 后。若同时启用 v2，v2 优先级更高。 |
| `enable_nat_port_map` | `bool` | `true` | `true` / `false` | 启用 UPnP / NAT-PMP 端口映射。自动向路由器请求端口转发。 |
| `enable_hole_punching` | `bool` | `true` | `true` / `false` | 启用 NAT 打洞。使 NAT 后的节点能建立直接连接，无需中继。 |
| `enable_auto_relay` | `bool` | `true` | `true` / `false` | 启用自动中继发现。自动寻找网络中继服务器并通告中继地址。 |
| `enable_auto_nat_v2` | `bool` | `true` | `true` / `false` | 启用 AutoNAT v2。更高效的 NAT 类型检测协议。 |
| `conn_mgr_low` | `int` | `100` | ≥0 | 连接数下限。连接数低于此值时，连接管理器不主动清理。 |
| `conn_mgr_high` | `int` | `400` | ≥ `conn_mgr_low` | 连接数上限。超过此值时，连接管理器按 LRU 淘汰闲置连接。 |
| `conn_mgr_grace` | `duration` | `20s` | Go duration | 连接优雅关闭时间。新连接在此时间内受保护，不被淘汰。 |
| `private_key` | `string` | `""` | PEM 格式 Ed25519 私钥 | 自定义 libp2p 身份私钥。为空时自动生成随机密钥对（不持久化）。 |

**支持的传输层：** TCP、WebSocket、QUIC v1、WebTransport (QUIC)、WebRTC Direct

**安全传输：** Noise、TLS

**多路复用：** Yamux

**局域网发现：** mDNS（自动启用，服务标识 `ipfar-bridge`）

---

### 8. CLI 标志参考

以下参数通过 `ipfar serve` 命令行标志设置，优先级高于 `config.json` 中的对应字段。

| 标志 | 简写 | 类型 | 默认值 | 说明 |
|------|------|------|--------|------|
| `--config` | `-c` | `string` | `"config.json"` | 配置文件路径 |
| `--port` | `-p` | `int` | `8080` | HTTP API 服务监听端口 |
| `--verbose` | `-v` | `bool` | `false` | 启用详细输出（日志级别临时设为 `debug`） |
| `--preset` | — | `string` | `"light"` | 安全预设：`strict` / `balanced` / `light` / `trusted` |
| `--gateway` | — | `string` | `"https://arweave.net"` | Arweave 网关 URL（单地址，覆盖 `gateway_list`） |
| `--cache-dir` | — | `string` | `"cache/car"` | CAR 文件和索引缓存目录 |
| `--online-verify` | — | `bool` | `false` | 启用在线验证（路径 A：HTTP Range 采样，不存盘） |
| `--online-sample` | — | `int` | `5` | 在线验证随机采样 block 数量 |
| `--online-concurrency` | — | `int` | `4` | 在线验证最大并发数 |
| `--download-concurrency` | — | `int` | `2` | 完整下载（路径 B）最大并发数 |
| `--dht` | — | `bool` | `true` | 启用 DHT 内容发布 |
| `--dht-mode` | — | `string` | `"server"` | DHT 模式：`server` / `client` |
| `--dht-reprovide` | — | `string` | `"12h"` | DHT 重新提供间隔 |
| `--dht-concurrency` | — | `int` | `4` | DHT 提供并发数 |
| `--discovery` | — | `string` | `"graphql-scan"` | 发现模式：`sampling` / `graphql` / `graphql-scan` |
| `--min-height` | — | `int` | `1919626` | 最小扫描区块高度 |
| `--scan-batch` | — | `int` | `100` | GraphQL 扫描批次大小 |
| `--scan-delay` | — | `int` | `2000` | GraphQL 扫描查询延迟（毫秒） |

---

## 配置加载优先级

1. **CLI 标志**（最高优先级，直接覆盖 ServiceConfig 字段）
2. **`config.json` 文件**（JSON 序列化到 `Config` 结构体）
3. **嵌入默认值**（文件不存在时从二进制嵌入的 `config/defaults/config.json` 释放）
4. **代码硬编码默认值**（最低优先级，`DefaultConfig()` / `DefaultHostConfig()` 等函数）

---

## 目录结构约定

| 目录/文件 | 用途 |
|-----------|------|
| `config.json` | 主配置文件 |
| `cache/car/` | CAR 文件缓存（默认） |
| `cache/car/index/` | Badger 索引数据库（CID → Arweave 映射） |
| `cache/car/blocks/` | Bitswap 数据块缓存 |
| `logs/ipfar.log` | 日志文件（默认路径） |

---

## 环境变量

桥节点当前不使用环境变量进行配置。所有配置均通过 `config.json` 文件或 CLI 标志传入。
