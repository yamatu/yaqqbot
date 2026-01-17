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

