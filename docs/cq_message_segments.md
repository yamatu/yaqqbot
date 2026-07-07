# CQ 消息段速查 & 图片输入（给 Codex）实现说明

本项目使用 OneBot v11 风格事件（`post_type/message`），消息正文主要通过 `raw_message`（CQ 码）解析。

## CQ 消息段列表（速查）

以下是常见 CQ 消息段类型（你提供的列表，便于后续扩展/排查）：

- `text`：纯文本
- `face`：QQ 表情
- `image`：图片
- `record`：语音
- `video`：视频
- `at`：@某人
- `rps`：猜拳
- `dice`：骰子
- `shake`：窗口抖动（仅收）
- `poke`：戳一戳（事件上报/接口，不通过消息段）
- `share`（<JSON>）：链接分享
- `contact`（<JSON>）：推荐好友/群
- `location`（<JSON>）：位置
- `music`（<JSON>）：音乐分享
- `reply`：回复消息
- `forward`：转发消息
- `node`：转发节点
- `json`：json 信息
- `mface`：QQ 表情包（部分实现会以 `image` 消息段上报，通过子类型区分；也可能直接是 `mface` 段）
- `file`：文件
- `markdown`：markdown（部分实现：只能在“双层合并转发”里发，无法直接发送）
- `lightapp`（<JSON>）：小程序卡片（发通常需扩展接口 `get_mini_app_ark`）

## 本项目当前的图片输入实现（重要）

目标：用户在 QQ 发“文字 + 图片”，机器人把图片保存到本地（绝对路径），再用 **Codex CLI** 的 `--image` 把图片作为附件发给模型，让模型识图。

实现位置：

- `main.go`：从 CQ 码中提取图片、下载/保存到临时目录、把绝对路径传给 AI 调用。
- `internal/codexcli/codexcli.go`：封装 `codex exec`，支持 `--image <FILE>`。

### 1) 从 CQ 码提取图片

当前用正则匹配 CQ 段：

- `[CQ:image,...]`
- `[CQ:mface,...]`（兼容某些实现）

然后解析参数（按逗号分隔的 `k=v`）：

- `url=`：如果存在，会用 HTTP GET 下载图片到临时目录
- `file=base64://...`：如果是 base64，会解码并写入临时目录
- `file=`：若是**本机绝对路径**且存在，直接作为附件路径传给 codex

CQ 码反转义会处理：

- `&amp;`、`&#91;`、`&#93;`、`&#44;`

### 2) 保存图片到本机并生成绝对路径

图片会写入：

- `<系统临时目录>/yaqqbot-images/qqimg-*.{jpg|png|...}`

写入后得到的文件名即绝对路径（会传给 `codex exec --image`）。

为避免磁盘堆积：本次对话处理完会自动删除这些临时图片文件（记忆里也不会保存绝对路径）。

### 3) 调用 Codex CLI 让模型识图

当“本次对话包含图片附件”时：

- 强制走 GPT（Codex CLI）路径（避免 Claude/Grok 丢图）
- 调用形态等价于：

```
codex exec --image /abs/path/to/img1.jpg --image /abs/path/to/img2.png "你的对话提示词..."
```

注意：运行机器人机器上必须能直接执行 `codex`（或设置 `CODEX_CLI_BIN` 指定路径）。

## 群聊：@机器人 + 引用(reply) 图片（从消息源提取）

场景：你在群里“引用一条含图片的消息”，同时 `@机器人`，但你发出的这条消息本身可能只有：

- `[CQ:reply,id=123]`
- `[CQ:at,qq=机器人]`
- 一点点文字（或没有文字）

此时 `raw_message` 里不一定带 `[CQ:image,...]`，所以需要额外调用 OneBot 接口拉取“被引用的原消息”。

本项目的实现：

- 从当前 `raw_message` 里解析 `reply` 段拿到 `message_id`
- 通过 WebSocket action 调用 `get_msg` 获取原消息
- 从 `get_msg` 返回的 `message`（可能是 CQ 字符串，也可能是消息段数组）中提取 `image/mface`
- 下载/解码到本机临时目录后，把绝对路径作为 `--image` 传给 `codex exec`

## /img 与 /gimg：生成图片并发送到 QQ

命令：

- `/img 提示词` 或 `/img 1024x1024 提示词`：使用 OpenAI GPT 图片模型
- `/gimg 提示词` 或 `/gimg 1024x768 提示词`：使用 Gemini 图片模型

实现要点：

- `/img` 调用 OpenAI Images API：
  - `POST <gpt_api_base>/images/generations`
  - 默认模型：`gpt-image-2`
  - 支持 `gpt_image_model` 配置覆盖
- `/gimg` 调用 Gemini REST 接口：
  - `POST https://generativelanguage.googleapis.com/v1beta/models/<model>:generateContent`
  - Header：`x-goog-api-key: $GEMINI_API_KEY`
- 如果命令里写了 `宽x高`，会作为图片接口尺寸参数传入；Gemini 返回后还会重采样到指定像素尺寸

配置项（环境变量或 `configs/config.json`）：

- `GPT_API_KEY` / `gpt_api_key`
- `GPT_API_BASE` / `gpt_api_base`
- `GPT_IMAGE_MODEL` / `gpt_image_model`
- `GEMINI_API_KEY` / `gemini_api_key`
- `GEMINI_API_BASE` / `gemini_api_base`（默认 `https://generativelanguage.googleapis.com/v1beta`）
- `GEMINI_IMAGE_MODEL` / `gemini_image_model`（默认 `gemini-2.5-flash-image`）

## Steam / Agent / 网页截图命令

- `//whatch 名字或好友代码或SteamID或个人资料链接`：加入 Steam 监控（兼容原拼写，也支持 `//watch`）
- `//whatchrm 名字或SteamID`：删除 Steam 监控
- `//list`：列出当前监控对象
- `//friends SteamID或链接`：查看公开好友列表中在线和正在游戏的好友
- `//buy [关键词]`：查询 Steam 折扣；不填关键词时返回热门折扣
- `//web URL`：调用本机 Chrome/Chromium 的 headless 模式打开网页并返回截图；可用 `CHROME_BIN` 指定浏览器路径
- `/webcheck`：检查本机是否能找到 Chrome/Chromium/Edge 内核，并给出安装提示
- `/search 关键词`：通过 gemini-search-mcp 搜索普通网页
- `/news 关键词`：通过 gemini-search-mcp 搜索新闻
- `/searchcheck`：检查 gemini-search-mcp 的 OpenAI 兼容服务是否可用
- `/60s`：通过 60s API 发送“60s 读懂世界”图片
- `/ainews` 或 `/ai-news`：通过 60s API 获取 AI 资讯快报，并渲染为图片发送
- `/socks on|off|status`：动态开关外部请求代理；开启后搜索、模型请求、网页解析等使用配置的代理
- `/deepseek 问题` 或 `/set deepseek`：使用 DeepSeek agent 对话

Steam 配置：

- `STEAM_API_KEY` / `steam_api_key`：Steam Web API Key，必须只放在本机配置或环境变量
- `STEAM_API_KEY_DOMAIN` / `steam_api_key_domain`：申请 key 时登记的域名，如 `book.yamatu.xyz`
- `STEAM_MONITOR_GROUPS` / `steam_monitor_groups`：监控播报目标群号列表
- `STEAM_POLL_INTERVAL` / `steam_poll_interval`：轮询间隔，例如 `60s`

联网搜索配置：

- Windows 启动本机搜索服务：`powershell -ExecutionPolicy Bypass -File scripts/start_gemini_search_mcp.ps1`
- 也可以在 cmd 里运行：`scripts\start_gemini_search_mcp.bat`
- 脚本会自动创建 `%LOCALAPPDATA%\gemini-search-mcp\venv`，并安装 gemini-search-mcp；如果提示缺少 Python，先运行 `winget install Python.Python.3.12`
- gemini-search-mcp 需要 Chrome/Edge/Chromium；如果 Windows 机器没有浏览器内核，先运行 `winget install Google.Chrome`
- 如果首次启动遇到 `Google CAPTCHA during warmup`，先运行 `scripts\prime_gemini_search_chrome.bat`，在弹出的 Chrome 中完成验证后关闭或保留窗口，再正常运行 `scripts\start_gemini_search_mcp.bat`
- 如果无头模式仍触发 CAPTCHA，运行 `scripts\start_gemini_search_mcp_cdp.bat`；它会先打开可见 Chrome，等你完成验证并按回车后，通过 `CDP_URL=http://127.0.0.1:9222` 连接这个 Chrome 启动搜索服务
- 如需代理 Google 搜索侧流量，可设置 `$env:GEMINI_SEARCH_PROXY_SERVER="socks5://127.0.0.1:7890"`；脚本也会在该变量为空时复用 `SOCKS5_PROXY`
- macOS/Linux 可运行：`scripts/start_gemini_search_mcp.sh`
- `GEMINI_SEARCH_API_BASE` / `gemini_search_api_base`：gemini-search-mcp 的 OpenAI 兼容接口地址，默认 `http://127.0.0.1:8080`；也可填 `http://127.0.0.1:8080/v1` 或完整 `/v1/chat/completions`
- `GEMINI_SEARCH_API_KEY` / `gemini_search_api_key`：gemini-search-mcp 不需要真实 API Key；如果你前面套了鉴权网关再填写
- `GEMINI_SEARCH_MODEL` / `gemini_search_model`：默认 `gemini-search`
- 配置后任意 AI 模型对话都会自动附加联网搜索摘要，`/deepseek` 也会使用；显式搜索可用 `/search`、`/news`，连通性检查用 `/searchcheck`

60s 聚合 API 配置：

- `SIXTY_API_BASE` / `sixty_api_base`：默认 `https://60s.viki.moe`
- `/epic` 使用 `/v2/epic`
- `/60s` 使用 `/v2/60s?encoding=image-proxy`
- `/ainews` 使用 `/v2/ai-news`，并将返回内容渲染为图片

代理配置：

- `SOCKS5_PROXY` / `socks5_proxy`：支持 `socks5://127.0.0.1:1080`、`http://127.0.0.1:7890`
- `PROXY_ENABLED` / `proxy_enabled`：启动时是否默认开启
- `/socks on|off|status`：运行时动态切换

长回复会在超过 `LONG_FORWARD_THRESHOLD` / `long_forward_threshold` 后优先使用 OneBot 合并转发折叠；失败时回退为分段消息。QQ notice/request 事件（例如戳一戳、入群、撤回等）会写入对应用户事件记忆，agent 调用时会带入最近事件。
