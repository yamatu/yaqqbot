# Repository Guidelines

## 项目结构与模块组织
- 根目录下的 `qqbot_server.py` 承载全部核心逻辑：WebSocket 服务、第三方模型客户端、消息分段与记忆存储。
- 运行后会生成 `qqbot.log`（轮转日志）与 `user_memory.json`。该 JSON 同时维护用户历史与群级配置（`__group_settings__` 记录 `/bv on|off` 和 `/setall` 生成的默认模型），清理前请先备份。
- 建议在项目根目录创建 `tests/`、`configs/` 等子目录，将异步辅助函数或提示词模板抽离为模块，避免单文件膨胀。
- 模型提示词可通过根目录的 `gpt.txt`、`claude.txt`、`grok.txt` 配置，缺省时回退到内置默认值。

## 构建、测试与开发命令
- `python3 qqbot_server.py`：以默认配置启动异步 QQBot 服务，包含模型切换、分段推送等功能。
- `pip install -r requirements.txt`：在首次开发或依赖更新时安装所需库；如需 SOCKS 代理支持请同时安装 `aiohttp_socks`。
- `ruff check qqbot_server.py`：快速执行静态检查，捕获格式与潜在错误；可配合 `ruff format` 自动修复。
- `pytest -q`：运行单元与集成测试，异步测试需使用 `pytest-asyncio` 并标注 `@pytest.mark.asyncio`。
- 运行期命令：`/clear` 清空私聊记忆；`/bv on|off` 与 `/setall <模型>` 管理群聊的解析与默认模型；群聊直接 `@机器人` 输入文本将触发默认 `/ct` 对话；Grok 等模型的默认提示词来自对应 `*.txt`。

## 编码风格与命名约定
- 遵循 PEP 8，统一使用 4 空格缩进，异步协程函数以 `async def` 命名并用动宾短语描述行为，如 `handle_group_message`。
- 常量使用全大写下划线（如 `MAX_MESSAGE_CHARS`），模块级配置集中在文件顶部，敏感密钥通过环境变量注入。
- 复杂逻辑需添加中文注释，优先解释输入输出约束与边界条件，而非逐行描述。
- 模型提示词统一由外部文件加载（`gpt.txt`/`claude.txt`/`grok.txt`），未找到文件时回退到内置默认；调整语气或格式时优先修改对应文件。

## 测试规范
- 以 `tests/test_<功能>.py` 命名测试文件，并使用 `given_when_then` 风格的函数名，例如 `test_handle_private_message_when_user_not_allowed`。
- 覆盖关键路径：模型选择、消息分段、错误重试与网络异常；对外部 API 使用 `pytest-mock` 或 `respx` 创建桩服务。
- 保持测试运行时间 < 60s，必要时在 `pytest.ini` 中排除长时间依赖真实外部接口的用例。

## 提交与合并请求准则
- 仓库尚未记录 Git 历史，创建提交时请采用 `类型: 模块 - 摘要`（例如 `feat: bot - 支持消息分段发送`）并在正文列出要点与验证方式。
- Pull Request 描述需包含：变更背景、实现概览、测试结果（含命令输出摘要）以及截图或日志片段；若涉及敏感配置变更需额外列出回滚方案。

## 安全与配置提示
- 所有 API Key、JWT、代理地址必须从环境变量或加密配置注入，切勿硬编码在版本控制内；提交前使用 `rg -n "sk-" -g"*"` 自查是否遗留密钥。
- 部署到公网前请确认 SOCKS5 代理、Packycode 用户接口与第三方模型端点均可达，且日志中不包含用户隐私信息。
