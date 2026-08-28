<div align="center">

# 适用于 Claude Code 的 S3 MCP Relay

**在一个插件中提供 MCP 注册、运维指导与本地任务状态。**

[项目文档](../README.md) · 简体中文 · [English](README.md) · [日本語](README.ja.md)

</div>

该插件将 `s3-relay-mcp` 集成至 Claude Code，并提供安全使用远程命令、分离式任务和文件传输所需的运维指导。

控制端通过 `s3-relay-mcp init` 初始化，通过 `add <pairing-code>` 加入 Agent。
任务输出与 Agent 运行日志会加密汇聚到控制端日志目录，并由 SQLite 建立事件和偏移索引；插件配置本身不包含任何密钥。

## 包含内容

| 组件 | 用途 |
|---|---|
| `.mcp.json` | 注册 `s3-relay-mcp`，安装插件即可完成全部设置 |
| `skills/relay-ops` | 说明关键失败模式：`exec` 与 `start_job` 的适用差异、超时不代表“未执行”、以及为何不该用 `read_file` 读取大文件 |
| `commands/relay-status.md` | `/relay-status`，用于查看 agent、任务、近期错误 |
| `scripts/hud-segment.sh` | 从缓存状态文件读取一行状态用于状态栏展示 |

`relay-ops` skill 用于补充 MCP 工具 schema 无法完整表达的运维语义，包括歧义性超时、分离式任务选择和上下文安全的文件传输。

## 前置条件

插件不包含平台相关二进制或凭据。请先使用 `deploy/install-controller.sh` 构建并安装控制端，然后在启用插件前设置以下环境变量：

```sh
export RELAY_MCP_BIN="$HOME/.local/bin/s3-relay-mcp"
export RELAY_CONFIG="$HOME/.config/relay/controller.toml"
export RELAY_LOG_FILE="$HOME/.local/state/relay/controller.log"
```

> [!WARNING]
> 如果 `install-controller.sh` 已经直接注册 MCP server，请在启用插件前删除或禁用该注册。重复加载同一 server 会导致工具注册冲突。

## 状态行

控制端每 30 秒刷新一次本地 JSON 状态文件，因此状态栏无需发起 MCP 请求或占用模型回合即可显示 agent 与任务状态。

```sh
plugin/scripts/hud-segment.sh
# ⬢ 2 agents · ⚙ 1 job
```

在 `claude-hud` 中，可将该脚本配置为自定义命令段；tmux 或 shell 提示符可使用相同调用方式。状态文件不存在或过期时，脚本不会输出内容。

`jq` 存在时会被用来解析；不存在时会退化为只输出 agent 数量。`--format=json` 会原样透传文件内容，方便外部 HUD 再自行解析。

## 状态文件结构

```json
{
  "updated_at": 1735000000,
  "interval_secs": 30,
  "agents": [
    {
      "id": "legacy-01",
      "hostname": "gpu-box",
      "os": "linux x86_64",
      "version": "0.1.0",
      "last_seen_secs": 4,
      "jobs_running": 1,
      "jobs_finished": [
        { "job": "…", "label": "resnet50", "state": "succeeded", "exit_code": 0, "finished_at": 1734999000 }
      ],
      "errors": []
    }
  ]
}
```

以 `updated_at` 作为新鲜度准则。`last_seen_secs` 超过约 45 秒说明心跳可能过期；该机器可能离线，或 agent 无法访问 S3。
