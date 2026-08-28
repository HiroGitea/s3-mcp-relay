<div align="center">

# S3 MCP Relay

**通过 S3 兼容对象存储，安全运维无法直连的 Linux 主机。**

无需开放入站端口，无需 VPN、堡垒机或双向直连网络。
端到端加密确保对象存储仅承载临时密文。

[![Rust](https://img.shields.io/badge/Rust-1.94%2B-000?style=flat-square&logo=rust)](https://www.rust-lang.org/)
[![MCP](https://img.shields.io/badge/Protocol-MCP-2563eb?style=flat-square)](https://modelcontextprotocol.io/)
[![License](https://img.shields.io/badge/License-MIT-16a34a?style=flat-square)](../LICENSE)

[English](../README.md) · 简体中文 · [日本語](README.ja.md)

[项目概览](#项目概览) · [安全模型](#安全模型) · [快速开始](#快速开始) · [工具列表](#工具列表) · [插件](#插件)

</div>

```text
  MCP 客户端 ──stdio── s3-relay-mcp ──HTTPS──▶ ┌──────────────┐
                                               │  S3 兼容存储  │
  relay-agent ◀───────── HTTPS 轮询 ────────── └──────────────┘
  （隔离服务器）
```

两端均只向同一 bucket 发起**出站** HTTPS 请求，控制端与隔离主机之间无需建立直接连接。

---

## 项目概览

部分环境明确禁止所有入站连接，但仍允许访问经过批准的对象存储。在此类环境中，VPN、堡垒机或开放 SSH 端口等传统方案可能不可用，或违反既有安全策略。

S3 MCP Relay 将这条已获批准的出站通道用作受限控制平面。控制端与远程 agent 通过专用 bucket 或 prefix 交换认证密文，无需建立直接网络路径。

## 核心能力

| 能力 | 说明 |
|---|---|
| **执行程序** | `exec` 跑短命令，`start_job` 跑以小时计的任务 |
| **传输文件** | 小文件走内联，大文件流经 bucket，**不经过对话** |
| **管理路径** | 读、写、列目录、创建、删除、移动 |
| **上报状态** | 心跳、近期错误、任务结果、机器指标——不用问就能看到 |

## 安全模型

- **端到端加密。** 每个对象都用 XChaCha20-Poly1305 加密，S3 对象 key 作为 AAD 参与认证，所以合法密文无法被搬到另一个 agent 的信箱里重放。存储服务商只看得到密文——服务端加密保护的是磁盘，挡不住服务本身。
- **强制 HTTPS。** 明文端点会被拒绝，除非为本地测试显式开启。
- **临时传输。** 消费方成功读取后删除对象。bucket 被设计为传输层，而非历史存储。
- **按 agent 隔离。** 每个 agent 用自己的凭据和限定在自己 prefix 上的 IAM 策略。
- **默认受限。** 命令有效期、协议版本校验、执行超时、输出上限、路径限定，以及一份拒绝把凭据传给子进程的环境变量白名单。

### 两种能力模式

**受限模式**（默认）：`exec` 只能运行白名单里精确匹配的绝对路径，不经过 shell；文件操作限定在声明的根目录内。

> [!WARNING]
> **全功能模式**（`allow_any_path` + `allow_any_program`）会提供与本地 shell 相当的远程代码执行能力。仅当 bucket、控制端、agent 和 MCP 会话处于同一可信管理边界内时启用；不要在共享主机上使用。

## 快速开始

### 前置条件

| 项目 | 最低要求 |
|---|---|
| Rust 工具链 | Rust 1.94 或更高版本 |
| 对象存储 | S3 兼容端点及专用 bucket 或 prefix |
| 远程主机 | 使用 systemd 或 OpenRC 的 Linux |
| MCP 客户端主机 | 支持本地 MCP 的客户端，例如 Claude Code 或 Codex |

### 1. 编译

```bash
cargo build --release --workspace
```

### 2. 安装 agent

在隔离 Linux 主机上执行：

```bash
sudo sh deploy/install-agent.sh \
  --agent-id legacy-01 \
  --endpoint https://cn-sy1.rains3.com \
  --bucket my-relay-bucket \
  --access-key AK... --secret-key SK...
```

脚本会自动识别 systemd 或 OpenRC，写入 `0600` 权限的配置，生成共享密钥并打印一次。

### 3. 安装控制端

在 MCP 客户端所在主机上执行：

```bash
sh deploy/install-controller.sh \
  --agents legacy-01 \
  --endpoint https://cn-sy1.rains3.com \
  --bucket my-relay-bucket \
  --access-key AK... --secret-key SK... \
  --shared-key <第 2 步打印的那串>
```

控制端按用户级安装，无需 root。若检测到 `claude` CLI，安装程序可自动注册 MCP server。

### 4. 确认连通性

重启 MCP 客户端并调用 `list_agents`。

## 工具列表

| 工具 | 说明 |
|---|---|
| `list_agents` | 在线 agent、任务状态、近期错误、机器指标 |
| `ping` | 往返探活 |
| `exec` | 单个程序，不经 shell，参数不做展开 |
| `start_job` · `list_jobs` · `job_output` · `cancel_job` | 分离式长任务 |
| `read_file` · `write_file` | ≤1 MiB，内容会进入对话 |
| `push_file` · `pull_file` | 任意大小，流经 bucket |
| `list_dir` · `make_dir` · `remove` · `move_path` | 路径管理 |

### 工具选择

> [!IMPORTANT]
> `exec` 仅适用于短命令。达到控制端超时后，远程进程仍可能在无人监管的情况下继续运行。构建、训练、导入及任何可能持续数分钟的操作均应使用 `start_job`。

`read_file` 把内容放进工具返回值，也就是进入模型上下文——100 KB 大约是 4 万 token。超过几十 KB 就该用 `pull_file`。

## 长任务

`start_job` 立刻返回一个 job id，默认最多监管 6 小时。输出直接流式写入 agent 上的文件，所以几个 GB 的训练日志在任何一端都不占内存。

**任务完成状态不会主动推送给模型。** 这是 MCP 交互模型的固有属性，并非实现缺陷。agent 会将任务结果写入心跳，控制端每 30 秒刷新本地状态文件。结果可通过 `list_jobs`、`/relay-status` 或状态栏读取。

## 文件传输

```
push_file(agent_id="legacy-01",
          local_path="~/torch-2.4.0-cp311-linux_x86_64.whl",
          remote_path="/srv/app/wheels/torch-2.4.0.whl")
→ {"bytes": 198234112, "chunks": 24, "sha256": "…"}
```

文件切成 8 MiB 分片，逐片加密后暂存在按传输隔离的 prefix 下。无论文件多大，峰值内存都在 16 MB 左右。接收方先写到同目录的临时文件，SHA-256 校验通过后才 rename 就位——中断绝不会留下看起来完整的半个文件。分片在成功和失败后都会清理。

与命令不同，传输**不是** at-most-once：搬字节没有副作用，失败了直接重试就行。

## 运维语义

以下行为与传统远程 shell 不同，均属于预期设计。

**超时不代表命令没执行。** 投递是 at-most-once：agent 可能已经执行完，只是上传响应失败了。重试任何有副作用的操作之前，先用只读命令确认它是不是已经发生了。丢失的响应会出现在 `recent_errors` 里，就是为了这个。

**延迟真实存在但很小。** agent 活跃时每 200 ms 轮询一次，空闲时退避到 5 秒。发给空闲 agent 的命令可能会等上几秒。

**加机器需要重启。** `allowed_agents` 在启动时读一次。不在列表里的 agent，无论多健康都是隐形的。

## 配置

TOML 文件加环境变量，**环境变量始终优先**。

```toml
[s3]
endpoint = "https://cn-sy1.rains3.com"
bucket   = "my-relay-bucket"
prefix   = "relay-prod/"

[agent]
id = "legacy-01"
allow_any_path    = true
allow_any_program = true
```

配置文件可以包含密钥，因此必须限制为仅文件所有者可读。包含密钥的文件权限宽于 `0600` 时，进程会在启动阶段发出警告。未知字段会被直接拒绝，以防安全配置拼写错误后静默改变策略。

参见 [`relay.toml.example`](../relay.toml.example)。

## 插件

| 插件 | 内容 |
|---|---|
| [Claude Code 插件](../plugin/) | MCP 注册、`relay-ops` skill、`/relay-status` 命令与状态栏辅助脚本 |
| [Codex 插件](../codex-plugins/s3-relay/) | MCP 注册，以及 `relay-ops` 与 `relay-status` 两个 skill |

Skills 用于补充工具 schema 无法完整表达的运维语义，包括歧义性超时、长任务分离执行和上下文安全的文件传输。

## 实现要点

**门铃机制。** 列举 prefix 是空闲 agent 最贵的请求。控制端投递命令后覆写一个门铃对象；agent 只对这一个 key 做 HEAD，ETag 变了才去列举。定期的全量扫描兜住门铃丢失的情况。

**自适应轮询。** 活跃时 200 ms，空闲退避到 5 秒，有活立刻回到最快档。Claude 的调用天然是突发的，这个节奏正好契合。

**心跳独立成任务**，这是刻意的：命令串行执行且可能跑几小时，共用一个循环会让心跳过期，把忙碌的 agent 显示成掉线。

**Key 布局。** `cmd/<agent>/`、`resp/<agent>/`、`agents/<agent>.json`、`doorbell/<agent>.json`、`blob/<agent>/<transfer>/`。

## 存储要求

临时传输语义依赖以下 bucket 配置：

1. 专用 bucket 或专用 prefix，**关闭** versioning 和 Object Lock，否则删除只产生 delete marker。
2. 配一条短生命周期规则，作为宕机时的兜底。
3. 不开复制、归档，以及包含 payload 的访问日志。
4. 服务端加密可以保留，但它替代不了端到端那一层。
5. 用 bucket policy 强制 TLS，控制端和 agent 使用独立身份。

如果服务商不得接收任何瞬时密文，则对象存储不适合作为传输介质。此时应使用经过审查的内存消息系统；该属性无法仅由客户端代码保证。

## 目录结构

```text
crates/common       协议、加密、配置、S3 传输、文件传输
crates/controller   MCP stdio server（s3-relay-mcp）
crates/agent        隔离服务器上的 agent（relay-agent）
deploy/             安装脚本、IAM 示例、systemd unit
plugin/             Claude Code 插件
codex-plugins/      Codex 插件
```

## 许可证

本项目采用 [MIT License](../LICENSE) 发布。
