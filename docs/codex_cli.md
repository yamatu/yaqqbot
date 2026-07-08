# Codex CLI 中转（codex-api.packycode.com）使用说明

## 背景

`https://codex-api.packycode.com/v1` 会返回类似：

> This API endpoint is only accessible via the official Codex CLI

这意味着它**不支持**被普通 HTTP 客户端（你的 Go 程序）直接调用。

本项目已做适配：当你显式开启 `gpt_use_codex_cli/GPT_USE_CODEX_CLI`，并且
`gpt_api_base/GPT_API_BASE` 指向 `codex-api.packycode.com` 时，程序会调用本机官方
**Codex CLI**（`codex exec`）来完成对话，从而合法使用该中转。

注意：Codex CLI 是完整代理运行方式，普通 QQ 闲聊默认禁用该路径。否则一句短问也可能带来
明显高于普通 Chat Completions 的上下文和代理开销。

## 兼容说明（重要）

不同版本的 Codex CLI 参数位置不一致：你本机 `codex -h` 显示 `--config/--sandbox/--ask-for-approval/--cd/--model`
是**全局参数**（必须放在 `exec` 之前），否则会出现 `unexpected argument`。

本项目会在运行时读取 `codex -h` / `codex exec --help`，自动把参数放到正确的位置并做回退。

## 常见报错：unexpected argument

如果你看到类似：

- `unexpected argument '--ask-for-approval' found`

说明你本机安装的 `codex` 版本较旧（或不是同一个发行版），不支持部分新参数。

本项目已改为**运行时读取 `codex exec --help` 自动适配参数**，但仍建议：

- 尽量升级/重装到你期望的官方 Codex CLI 版本（并确保 `codex exec --help` 输出与你安装一致）

## 常见报错：missing field `name`

如果你看到类似：

- `Error loading config.toml: missing field name in model_providers.xxx`

说明你本机 codex 的配置结构要求 provider 必须包含 `name` 字段。
本项目已在运行时覆盖配置时自动补上该字段，无需你手动修改。

## 你需要做什么

1. 安装官方 Codex CLI（需要 Node.js 环境）
   - `npm i -g @openai/codex`
2. 确认命令可用
   - `codex --version`
3. 在你的 `configs/config.json` 中保持（或设置）：
   - `gpt_api_base`: `https://codex-api.packycode.com/v1`
   - `gpt_api_key`: 你的中转 Key（程序会把它注入给 Codex CLI 使用）
   - `gpt_use_codex_cli`: `true`
4. 启动
   - `go run .`
   - 或 `go run main.go`

## 可选环境变量

- `CODEX_CLI_BIN`：指定 Codex CLI 可执行文件名/路径（默认 `codex`）
- `CODEX_WIRE_API`：Codex CLI 使用的底层接口类型（默认 `responses`，必要时可改为 `chat`）
- `CODEX_SKIP_GIT_REPO_CHECK`：是否追加 `--skip-git-repo-check`（默认开启；设为 `0/false/off` 可关闭）
- `GPT_USE_CODEX_CLI`：是否允许 GPT 普通聊天走 Codex CLI（默认关闭）

## Windows 说明

Codex CLI 在 Windows 原生环境可能受限；如果遇到不可用，建议在 WSL / Linux 环境运行机器人服务。

另外 Windows 下命令行长度有限，如果对话历史很长，把整段 prompt 作为命令行参数可能触发“参数/输入太长”。
本项目已默认优先通过 stdin 传入 prompt 来规避该问题；如果你使用的 codex 版本不支持 stdin，
程序会自动回退到短参数模式（例如使用 `-` 作为占位），或提示你升级 codex。
