<div align="center">

# Claude Code 向け S3 MCP Relay

**MCP 登録、運用ガイダンス、ローカルジョブ状態を 1 つのプラグインで提供します。**

[プロジェクト文書](../README.md) · [English](README.md) · [简体中文](README.zh-CN.md) · 日本語

</div>

このプラグインは `s3-relay-mcp` を Claude Code に統合し、リモートコマンド、分離ジョブ、ファイル転送を安全に使用するための運用ガイダンスを提供します。

## パッケージ内容

| コンポーネント | 目的 |
|---|---|
| `.mcp.json` | `s3-relay-mcp` を登録し、プラグインのインストールだけで初期設定を完了させる |
| `skills/relay-ops` | 失敗モードを説明します。`exec` と `start_job` の使い分け、タイムアウトが「未実行」を意味しない理由、`read_file` を大きなファイルで使うべきでない理由 |
| `commands/relay-status.md` | `/relay-status` — エージェント、ジョブ、最近のエラー |
| `scripts/hud-segment.sh` | キャッシュ済みステータスファイルから 1 行のステータスラインを生成 |

`relay-ops` skill は、曖昧なタイムアウト、分離ジョブの選択、コンテキストを消費しないファイル転送など、MCP ツールスキーマだけでは十分に表現できない運用上の判断基準を補完します。

## 前提条件

プラグインにはプラットフォーム固有のバイナリや認証情報は含まれません。`deploy/install-controller.sh` でコントローラーをビルドおよびインストールし、プラグインを有効にする前に以下の環境変数を設定します。

```sh
export RELAY_MCP_BIN="$HOME/.local/bin/s3-relay-mcp"
export RELAY_CONFIG="$HOME/.config/relay/controller.toml"
export RELAY_LOG_FILE="$HOME/.local/state/relay/controller.log"
```

> [!WARNING]
> `install-controller.sh` が MCP サーバーを直接登録済みの場合、プラグインを有効にする前にその登録を削除または無効化してください。同一サーバーを重複して読み込むと、ツール登録が競合します。

## ステータスライン

コントローラーは 30 秒ごとにローカル JSON ステータスファイルを更新します。そのため、ステータスラインは MCP リクエストやモデルターンを使用せずにエージェントとジョブの状態を表示できます。

```sh
plugin/scripts/hud-segment.sh
# ⬢ 2 agents · ⚙ 1 job
```

`claude-hud` ではカスタムコマンドセグメントとして設定します。tmux やシェルプロンプトでも同じコマンドを使用できます。状態ファイルが存在しないか古い場合、スクリプトは何も出力しません。

`jq` がある場合はそれを使ってパースし、無い場合は agent 数のみを出力してフォールバックします。`--format=json` は HUD が独自に解析できるよう、ファイル内容を生のまま通します。

## ステータスファイルの構造

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

`updated_at` を最新性の基準として扱ってください。`last_seen_secs` が約 45 秒を超えると、agent のハートビートが古い可能性があります。機器が落ちているか、agent が S3 に届かない状態の可能性があります。
