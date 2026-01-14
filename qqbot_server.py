import asyncio
import asyncio.subprocess as asp
import websockets
import json
import time
from datetime import datetime
from anthropic import AsyncAnthropic
from openai import AsyncOpenAI
import aiohttp
from aiohttp_socks import ProxyConnector
import re
from scrapy import Selector
import base64
import jmespath
import logging
from logging.handlers import RotatingFileHandler
import socket
import os
import html
import shutil

# 尝试导入 curl_cffi (用于绕过某些 API 的 Cloudflare 防护)
try:
    from curl_cffi import requests as curl_requests
    HAS_CURL_CFFI = True
except ImportError:
    HAS_CURL_CFFI = False

# ============ 日志配置 ============
logger = logging.getLogger('QQBot')
logger.setLevel(logging.INFO)

file_handler = RotatingFileHandler(
    'qqbot.log', maxBytes=10*1024*1024, backupCount=5, encoding='utf-8'
)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

# ============ 配置区域 ============
# 说明：敏感信息（API Key/JWT/Token）不要写死在代码里，统一从环境变量或本地 configs/config.json 读取

def _load_local_config(config_path: str) -> dict:
    """加载本地配置文件（默认 configs/config.json），文件通常被 .gitignore 忽略。"""
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except Exception as e:
        logger.warning("读取配置文件失败：%s（路径：%s）", e, config_path)
        return {}


def _get_env_any(names: list[str], default: str = "") -> str:
    """按优先级读取多个环境变量名，取第一个非空值。"""
    for name in names:
        val = os.getenv(name)
        if val:
            return val
    return default


CONFIG_PATH = os.getenv("QQBOT_CONFIG_PATH", os.path.join("configs", "config.json"))
LOCAL_CONFIG = _load_local_config(CONFIG_PATH)

TOKEN = _get_env_any(["QQBOT_TOKEN", "TOKEN"], str(LOCAL_CONFIG.get("token", "")))
# 允许使用机器人的QQ号白名单（私聊）
ALLOWED_QQ_USERS = ["984346643", "836644146", "3541975032"]

# Claude API配置
CLAUDE_API_KEY = _get_env_any(
    ["CLAUDE_API_KEY", "QQBOT_CLAUDE_API_KEY"],
    str(LOCAL_CONFIG.get("claude_api_key", "")),
)
CLAUDE_API_BASE = _get_env_any(
    ["CLAUDE_API_BASE", "QQBOT_CLAUDE_API_BASE"],
    str(LOCAL_CONFIG.get("claude_api_base", "https://agentrouter.org")),
)
CLAUDE_MODEL = _get_env_any(
    ["CLAUDE_MODEL", "QQBOT_CLAUDE_MODEL"],
    str(LOCAL_CONFIG.get("claude_model", "claude-sonnet-4-5-20250929")),
)

# GPT API配置
GPT_API_KEY = _get_env_any(
    ["GPT_API_KEY", "QQBOT_GPT_API_KEY"],
    str(LOCAL_CONFIG.get("gpt_api_key", "")),
)
GPT_API_BASE = _get_env_any(
    ["GPT_API_BASE", "QQBOT_GPT_API_BASE"],
    str(LOCAL_CONFIG.get("gpt_api_base", "https://codex-api.packycode.com/v1")),
)
GPT_MODEL = _get_env_any(
    ["GPT_MODEL", "QQBOT_GPT_MODEL"],
    str(LOCAL_CONFIG.get("gpt_model", "gpt-5.1")),
)

# Grok API配置
GROK_API_KEY = _get_env_any(
    ["GROK_API_KEY", "QQBOT_GROK_API_KEY"],
    str(LOCAL_CONFIG.get("grok_api_key", "")),
)
GROK_API_BASE = _get_env_any(
    ["GROK_API_BASE", "QQBOT_GROK_API_BASE"],
    str(LOCAL_CONFIG.get("grok_api_base", "https://happyapi.org/v1")),
)
GROK_MODEL = _get_env_any(
    ["GROK_MODEL", "QQBOT_GROK_MODEL"],
    str(LOCAL_CONFIG.get("grok_model", "grok-3")),
)

# 上下文记忆与文件
MEMORY_FILE = "user_memory.json"
MAX_HISTORY_TURNS = 10
GROUP_SETTINGS_KEY = "__group_settings__"

# 提示词文件配置
PROMPT_FILES = {
    "grok": "grok.txt",
    "gpt": "gpt.txt",
    "claude": "claude.txt",
}

# API Keys & URLs
AMAP_API_KEY = _get_env_any(
    ["AMAP_API_KEY", "QQBOT_AMAP_API_KEY"],
    str(LOCAL_CONFIG.get("amap_api_key", "")),
)
BILIBILI_API_BASE = _get_env_any(
    ["BILIBILI_API_BASE", "QQBOT_BILIBILI_API_BASE"],
    str(LOCAL_CONFIG.get("bilibili_api_base", "https://api.bilibili.com/x/web-interface/view")),
)
PACKYCODE_USERINFO_URL = _get_env_any(
    ["PACKYCODE_USERINFO_URL", "QQBOT_PACKYCODE_USERINFO_URL"],
    str(LOCAL_CONFIG.get("packycode_userinfo_url", "https://codex.packycode.com/api/backend/users/info")),
)
PACKYCODE_USER_JWT = _get_env_any(
    ["PACKYCODE_USER_JWT", "QQBOT_PACKYCODE_USER_JWT"],
    str(LOCAL_CONFIG.get("packycode_user_jwt", "")),
)

# 代理配置
SOCKS5_PROXY = _get_env_any(
    ["SOCKS5_PROXY", "QQBOT_SOCKS5_PROXY"],
    str(LOCAL_CONFIG.get("socks5_proxy", "socks5://127.0.0.1:41457")),
)

# 群聊白名单
ALLOWED_GROUP_IDS = ["1021625874", "421953860", "827500600","1039488471"]

# 发送消息分片
MESSAGE_CHUNK_SIZE = 4000

# Nginx stream 配置目录
NGINX_STREAM_CONF_DIR = "/etc/nginx/stream.conf.d"

# 被控 Nginx 服务器配置文件
NGINX_SERVERS_FILE = "nginx_servers.json"

class QQBotServer:
    def __init__(self):
        self.shutdown_event = asyncio.Event()
        
        # 1. 内存管理
        self.user_memory = {}
        self.memory_dirty = False 
        self._load_memory()
        
        # 2. 加载 Prompt 文件
        self.model_prompts = {}
        self._load_model_prompts()
        
        # 3. 预编译正则
        self.re_cq = re.compile(r'\[CQ:[^\]]+\]')
        self.re_bv = re.compile(r'BV[a-zA-Z0-9]{10}')
        self.re_bilibili_url = re.compile(r'(https?://)?((www|m)\.)?bilibili\.com/video/(BV[a-zA-Z0-9]{10})')
        self.re_youtube = [
            re.compile(r'(?:https?://)?(?:www\.)?youtube\.com/watch\?v=([a-zA-Z0-9_-]{11})'),
            re.compile(r'(?:https?://)?(?:www\.)?youtu\.be/([a-zA-Z0-9_-]{11})'),
            re.compile(r'watch\?v=([a-zA-Z0-9_-]{11})')
        ]
        # 【新增】Ping 域名提取正则：匹配 http(s):// 以及路径和端口，只提取主机名
        # 逻辑：忽略协议前缀 -> 捕获主机名(直到遇到 / : ? # 或结尾)
        self.re_host_extract = re.compile(r'(?:https?://)?(?P<host>[^/:\s\?#]+)')
        
        # 4. 被控 Nginx 服务器列表与权限
        self.nginx_servers = {}               # {name: "host:port"}
        self.nginx_default_server = None      # 当前默认操作的被控服务器名
        self.nginx_server_confs = {}          # 每个被控服务器对应的配置文件名 {name: conf_name}
        self.nginx_acl = {}                   # 服务器权限映射 {name: [qq1, qq2, ...]}
        self._load_nginx_servers()

        # 5. 初始化占位符
        self.claude_client = None
        self.gpt_client = None
        self.session = None
        self.proxy_session = None
        
        self.default_model = "gpt"

    def _init_network_clients(self):
        """初始化网络客户端 (必须在 asyncio loop 运行后调用)"""
        # Claude
        self.claude_client = AsyncAnthropic(api_key=CLAUDE_API_KEY, base_url=CLAUDE_API_BASE)
        
        # GPT
        import httpx
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36",
            "Origin": GPT_API_BASE.rstrip('/'),
            "Referer": f"{GPT_API_BASE.rstrip('/')}/"
        }
        ipv4_transport = httpx.AsyncHTTPTransport(local_address="0.0.0.0")
        ipv4_client = httpx.AsyncClient(transport=ipv4_transport, headers=headers, timeout=180.0, follow_redirects=True)
        self.gpt_client = AsyncOpenAI(
            api_key=GPT_API_KEY, base_url=GPT_API_BASE, http_client=ipv4_client, timeout=180.0
        )

        # 通用 Session
        connector = aiohttp.TCPConnector(family=socket.AF_INET, limit=100)
        self.session = aiohttp.ClientSession(connector=connector)

        # 代理 Session
        if SOCKS5_PROXY:
            try:
                proxy_conn = ProxyConnector.from_url(SOCKS5_PROXY)
                self.proxy_session = aiohttp.ClientSession(connector=proxy_conn)
            except Exception:
                logger.warning("代理初始化失败，降级直连")
                self.proxy_session = aiohttp.ClientSession()
        else:
            self.proxy_session = aiohttp.ClientSession()

    async def cleanup(self):
        if self.session and not self.session.closed: await self.session.close()
        if self.proxy_session and not self.proxy_session.closed: await self.proxy_session.close()
        self._save_memory(force=True)

    # ============ 提示词与配置管理 ============
    def _load_model_prompts(self):
        defaults = {
            "grok": "你是Grok，语气轻松幽默。",
            "gpt": "你是AI助手，请简洁回答。",
            "claude": "你是Claude，请详细且逻辑清晰地回答。"
        }
        for model, filename in PROMPT_FILES.items():
            try:
                if os.path.exists(filename):
                    with open(filename, 'r', encoding='utf-8') as f:
                        self.model_prompts[model] = f.read().strip()
                        logger.info(f"已加载提示词文件: {filename}")
                else:
                    self.model_prompts[model] = defaults.get(model, "")
            except Exception as e:
                self.model_prompts[model] = defaults.get(model, "")

    # ============ 内存管理 ============
    def _load_memory(self):
        if os.path.exists(MEMORY_FILE):
            try:
                with open(MEMORY_FILE, 'r', encoding='utf-8') as f:
                    self.user_memory = json.load(f)
            except Exception:
                self.user_memory = {}
        self._ensure_group_settings()

    def _save_memory(self, force=False):
        try:
            with open(MEMORY_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.user_memory, f, ensure_ascii=False, indent=2)
            self.memory_dirty = False
        except Exception as e:
            logger.error(f"保存记忆失败: {e}")

    async def _background_save_task(self):
        while not self.shutdown_event.is_set():
            await asyncio.sleep(30)
            if self.memory_dirty:
                await asyncio.to_thread(self._save_memory)

    # ============ Nginx 被控服务器配置管理 ============
    def _load_nginx_servers(self):
        """加载被控 Nginx 服务器配置列表"""
        if os.path.exists(NGINX_SERVERS_FILE):
            try:
                with open(NGINX_SERVERS_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    # 兼容旧格式: 纯 {name: "host:port"} 映射
                    self.nginx_default_server = data.get("__default__")
                    confs = data.get("__conf__", {})
                    if isinstance(confs, dict):
                        self.nginx_server_confs = {
                            str(k): str(v) for k, v in confs.items()
                        }
                    acls = data.get("__acl__", {})
                    if isinstance(acls, dict):
                        self.nginx_acl = {
                            str(k): [str(x) for x in (v or [])]
                            for k, v in acls.items()
                            if isinstance(v, list)
                        }
                    # 过滤掉元数据键
                    self.nginx_servers = {
                        str(k): str(v)
                        for k, v in data.items()
                        if not str(k).startswith("__")
                    }
                else:
                    self.nginx_servers = {}
            except Exception as e:
                logger.error(f"加载被控服务器配置失败: {e}")
                self.nginx_servers = {}
        else:
            self.nginx_servers = {}

    def _save_nginx_servers(self):
        """保存被控 Nginx 服务器配置列表到本地文件"""
        try:
            data = {}
            if self.nginx_default_server:
                data["__default__"] = self.nginx_default_server
            if self.nginx_server_confs:
                data["__conf__"] = self.nginx_server_confs
            if self.nginx_acl:
                data["__acl__"] = self.nginx_acl
            data.update(self.nginx_servers)
            with open(NGINX_SERVERS_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"保存被控服务器配置失败: {e}")

    def _is_global_admin(self, user_id: str, msg_type: str) -> bool:
        """判断是否为全局管理员（仅限白名单私聊）"""
        return msg_type == "private" and user_id in ALLOWED_QQ_USERS

    def _get_default_nginx_server(self):
        """
        获取默认的被控 Nginx 服务器:
        - 如果设置了默认服务器，则返回该服务器
        - 否则若已配置至少一个服务器，则返回第一个 (name, host, port)
        - 如果未配置，则返回 None
        """
        if not self.nginx_servers:
            return None
        # 优先使用手动设置的默认服务器
        if self.nginx_default_server and self.nginx_default_server in self.nginx_servers:
            name = self.nginx_default_server
            addr = self.nginx_servers[name]
        else:
            try:
                name, addr = next(iter(self.nginx_servers.items()))
            except StopIteration:
                return None
        if ":" not in addr:
            return None
        host, port_str = addr.rsplit(":", 1)
        host = host.strip()
        if not host:
            return None
        try:
            port = int(port_str)
        except ValueError:
            return None
        return name, host, port

    def _ensure_group_settings(self):
        if GROUP_SETTINGS_KEY not in self.user_memory:
            self.user_memory[GROUP_SETTINGS_KEY] = {"bv_enabled": {}, "model_default": {}}

    def _ensure_user(self, qq):
        if qq not in self.user_memory:
            self.user_memory[qq] = {"history": [], "model": None}

    def _append_history(self, qq, role, content):
        self._ensure_user(qq)
        profile = self.user_memory[qq]
        profile.setdefault("history", []).append({"role": role, "content": content})
        max_items = MAX_HISTORY_TURNS * 2
        if len(profile["history"]) > max_items:
            profile["history"] = profile["history"][-max_items:]
        self.memory_dirty = True

    def clear_user_memory(self, qq):
        if qq in self.user_memory:
            self.user_memory[qq]["history"] = []
            self.memory_dirty = True
            return True
        return False

    # ============ 设置相关 ============
    def get_user_model(self, qq, group_id=None):
        if qq in self.user_memory and self.user_memory[qq].get("model"):
            return self.user_memory[qq]["model"]
        if group_id:
            defaults = self.user_memory[GROUP_SETTINGS_KEY].get("model_default", {})
            if str(group_id) in defaults:
                return defaults[str(group_id)]
        return self.default_model

    def set_user_model(self, qq, model):
        if model in ["claude", "gpt", "grok"]:
            self._ensure_user(qq)
            self.user_memory[qq]["model"] = model
            self.memory_dirty = True
            return True
        return False
    
    def get_group_default_model(self, group_id):
        if not group_id: return None
        return self.user_memory[GROUP_SETTINGS_KEY]["model_default"].get(str(group_id))

    def set_group_default_model(self, group_id, model):
        if not group_id: return
        self.user_memory[GROUP_SETTINGS_KEY]["model_default"][str(group_id)] = model
        self.memory_dirty = True

    def is_group_bv_enabled(self, group_id):
        if not group_id: return True
        settings = self.user_memory[GROUP_SETTINGS_KEY].get("bv_enabled", {})
        return settings.get(str(group_id), True)
    
    def set_group_bv_enabled(self, group_id, enabled: bool):
        if not group_id: return
        self.user_memory[GROUP_SETTINGS_KEY]["bv_enabled"][str(group_id)] = enabled
        self.memory_dirty = True

    # ============ 消息辅助 ============
    def _strip_cq_codes(self, message):
        if not message: return ""
        return self.re_cq.sub('', message).strip()

    async def send_long_text(self, websocket, message_type, target_id, text):
        if not text: return
        chunks = [text[i:i+MESSAGE_CHUNK_SIZE] for i in range(0, len(text), MESSAGE_CHUNK_SIZE)]
        for part in chunks:
            payload = {
                "action": "send_private_msg" if message_type == "private" else "send_group_msg",
                "params": {
                    ("user_id" if message_type == "private" else "group_id"): target_id,
                    "message": part
                }
            }
            await websocket.send(json.dumps(payload))
            await asyncio.sleep(0.2)

    # ============ 功能 API ============

    # ---------- Nginx 管理相关 ----------

    async def _run_shell_command(self, *cmd):
        """
        使用异步子进程执行命令，返回 (退出码, 输出文本)
        """
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asp.PIPE,
                stderr=asp.STDOUT,
            )
            stdout, _ = await proc.communicate()
            output = stdout.decode("utf-8", errors="ignore")
            return proc.returncode, output.strip()
        except FileNotFoundError:
            return 127, f"命令不存在: {' '.join(cmd)}"
        except Exception as e:
            return 1, f"执行命令异常: {e}"

    async def reload_nginx(self):
        """
        尝试测试并重载 Nginx 配置：
        1. nginx -t 检查配置语法
        2. 优先尝试 nginx -s reload
        3. 失败则依次尝试 systemctl reload nginx / systemctl restart nginx
        """
        # 先测试配置
        code, output = await self._run_shell_command("nginx", "-t")
        if code != 0:
            return False, f"❌ nginx 配置测试失败 (nginx -t)\n{output}"

        tried = []
        for cmd in [
            ("nginx", "-s", "reload"),
            ("systemctl", "reload", "nginx"),
            ("systemctl", "restart", "nginx"),
        ]:
            code, out = await self._run_shell_command(*cmd)
            tried.append((cmd, code, out))
            if code == 0:
                return True, f"✅ 已执行: {' '.join(cmd)}\n{out}"

        msg_lines = ["❌ 所有 Nginx 重载命令均执行失败，已保留配置文件，请手动检查："]
        for cmd, code, out in tried:
            msg_lines.append(f"- {' '.join(cmd)} (exit {code})")
            if out:
                msg_lines.append(f"  输出: {out}")
        return False, "\n".join(msg_lines)

    async def nginx_test_config(self):
        """
        仅执行 nginx -t，供 /nginx -t 指令查看当前配置是否有效
        """
        code, output = await self._run_shell_command("nginx", "-t")
        if code == 0:
            prefix = "✅ nginx 配置语法检测通过 (nginx -t)\n"
        else:
            prefix = "❌ nginx 配置语法检测失败 (nginx -t)\n"
        return prefix + (output or "")

    def _parse_nginx_stream_summary(self, content, filename):
        """
        尝试解析 stream 配置文件中的所有 upstream/server 映射，返回多条摘要。
        例如:
        - jpp: jpp.yamatu.xyz:46569 -> 0.0.0.0:10197
        - 403: 141.11.50.93:8887 -> 0.0.0.0:10196
        """
        results = []

        # 1) 支持通过注释标记的规则 (可重复多次)
        meta_re = re.compile(
            r"^#\s*name=(?P<name>\S+)\s+target=(?P<host>[^:\s]+):(?P<tport>\d+)\s+listen=(?P<lport>\d+)",
            re.MULTILINE,
        )
        for m in meta_re.finditer(content):
            name = m.group("name")
            host = m.group("host")
            tport = m.group("tport")
            lport = m.group("lport")
            results.append(f"{name}: {host}:{tport} -> 0.0.0.0:{lport}")

        # 2) 通用解析：从 upstream + server 块中推导
        up_map = {}
        # 收集 upstream 中的目标地址
        for m in re.finditer(
            r"upstream\s+([a-zA-Z0-9_-]+)\s*{([^}]*)}", content, re.DOTALL
        ):
            up_name = m.group(1)
            body = m.group(2)
            s = re.search(
                r"server\s+([0-9a-zA-Z\.\-]+):(\d+);", body
            )
            if s:
                up_map[up_name] = (s.group(1), s.group(2))

        # 对每个 server 块，找到 proxy_pass 的 upstream 名称及 listen 端口
        seen = set()
        for m in re.finditer(r"server\s*{([^}]*)}", content, re.DOTALL):
            body = m.group(1)
            p = re.search(r"proxy_pass\s+([a-zA-Z0-9_-]+);", body)
            if not p:
                continue
            up_name = p.group(1)
            if up_name not in up_map:
                continue
            lp = re.search(
                r"listen\s+(?:[0-9\.\:]+\:)?(\d+)\b", body
            )
            if not lp:
                continue
            lport = lp.group(1)
            host, tport = up_map[up_name]
            key = (up_name, host, tport, lport)
            if key in seen:
                continue
            seen.add(key)
            results.append(f"{up_name}: {host}:{tport} -> 0.0.0.0:{lport}")

        if not results:
            return [f"{filename} (自定义配置，未解析详细信息)"]
        return results

    async def nginx_list_configs(self):
        """
        列出 /etc/nginx/stream.conf.d 中所有 .conf 配置的概况
        """
        conf_dir = NGINX_STREAM_CONF_DIR
        if not os.path.isdir(conf_dir):
            return f"❌ 目录不存在: {conf_dir}\n请确认已在 nginx.conf 中正确配置 include。"

        files = sorted(
            f for f in os.listdir(conf_dir) if f.endswith(".conf") and os.path.isfile(os.path.join(conf_dir, f))
        )
        if not files:
            return "📂 当前没有任何 stream 配置 (.conf)。"

        lines = ["📂 Nginx stream 配置列表:"]
        for fn in files:
            path = os.path.join(conf_dir, fn)
            try:
                with open(path, "r", encoding="utf-8", errors="ignore") as f:
                    content = f.read(4096)
                summaries = self._parse_nginx_stream_summary(content, fn)
                if isinstance(summaries, str):
                    summaries = [summaries]
                for s in summaries:
                    lines.append(f"- {s}  [{fn}]")
            except Exception as e:
                lines.append(f"- {fn}: 读取失败: {e}")
        return "\n".join(lines)

    async def nginx_add_config(self, name, target_host, target_port, listen_port):
        """
        新增或更新一个 stream 转发配置：
        /nginx add [名字] [转发地址] [目标端口] [本地端口]
        """
        conf_dir = NGINX_STREAM_CONF_DIR
        os.makedirs(conf_dir, exist_ok=True)

        # 简单校验参数，避免生成非法配置
        if not re.fullmatch(r"[a-zA-Z0-9_-]+", name):
            return "❌ 名字只允许使用字母、数字、下划线、短横线。"

        if " " in target_host or "/" in target_host:
            return "❌ 转发地址格式不合法，只允许域名或 IP，不要包含协议/http://。"

        try:
            t_port = int(target_port)
            l_port = int(listen_port)
            if not (1 <= t_port <= 65535 and 1 <= l_port <= 65535):
                return "❌ 端口号必须在 1-65535 之间。"
        except ValueError:
            return "❌ 端口号必须是数字。"

        conf_path = os.path.join(conf_dir, f"{name}.conf")

        # 如果已存在，做一个简单备份
        backup_msg = ""
        if os.path.exists(conf_path):
            ts = datetime.now().strftime("%Y%m%d%H%M%S")
            backup_path = conf_path + f".bak.{ts}"
            try:
                shutil.copy2(conf_path, backup_path)
                backup_msg = f"\nℹ️ 已备份旧配置为: {backup_path}"
            except Exception as e:
                backup_msg = f"\n⚠️ 旧配置备份失败: {e}"

        content = f"""# name={name} target={target_host}:{t_port} listen={l_port}  auto=qqbot
stream {{
    resolver 1.1.1.1 valid=300s ipv6=on;

    upstream {name} {{
        server {target_host}:{t_port};
    }}

    # 支持 TCP 协议
    server {{
        listen 0.0.0.0:{l_port};
        proxy_connect_timeout 5s;
        proxy_timeout 600s;
        proxy_pass {name};
    }}

    # 支持 UDP 协议
    server {{
        listen 0.0.0.0:{l_port} udp;
        proxy_connect_timeout 5s;
        proxy_timeout 600s;
        proxy_pass {name};
    }}
}}
"""
        try:
            with open(conf_path, "w", encoding="utf-8") as f:
                f.write(content)
        except PermissionError:
            return f"❌ 写入失败，没有权限写入 {conf_path}，请确保机器人进程具有 root 权限。"
        except Exception as e:
            return f"❌ 写入配置失败: {e}"

        ok, reload_msg = await self.reload_nginx()
        prefix = f"✅ 已写入配置: {conf_path}{backup_msg}\n"
        return prefix + reload_msg

    async def nginx_remove_config(self, name):
        """
        删除一个 stream 配置文件：/nginx rm [名字]
        """
        conf_dir = NGINX_STREAM_CONF_DIR
        conf_path = os.path.join(conf_dir, f"{name}.conf")

        if not os.path.exists(conf_path):
            return f"❌ 未找到配置文件: {conf_path}"

        # 删除前简单备份
        backup_msg = ""
        try:
            ts = datetime.now().strftime("%Y%m%d%H%M%S")
            backup_path = conf_path + f".bak.{ts}"
            shutil.copy2(conf_path, backup_path)
            backup_msg = f"\nℹ️ 已备份为: {backup_path}"
        except Exception as e:
            backup_msg = f"\n⚠️ 删除前备份失败: {e}"

        try:
            os.remove(conf_path)
        except PermissionError:
            return f"❌ 删除失败，没有权限删除 {conf_path}，请确保机器人进程具有 root 权限。"
        except Exception as e:
            return f"❌ 删除配置失败: {e}"

        ok, reload_msg = await self.reload_nginx()
        prefix = f"✅ 已删除配置: {conf_path}{backup_msg}\n"
        return prefix + reload_msg

    async def handle_nginx_qq_command(self, parts, user_id: str, msg_type: str):
        """
        处理 /nginx qq 子命令：
        /nginx qq add[addr] [服务器名] [QQ号]
        /nginx qq rm [服务器名] [QQ号]
        /nginx qq list [服务器名]
        """
        if not self._is_global_admin(user_id, msg_type):
            return "❌ 只有白名单私聊管理员可以管理 Nginx QQ 权限。"

        if not parts:
            return (
                "🔐 Nginx QQ 权限管理:\n"
                "/nginx qq add 服务器名 QQ   添加某 QQ 为该服务器编辑者\n"
                "/nginx qq rm 服务器名 QQ    删除某 QQ 的编辑权限\n"
                "/nginx qq list [服务器名]   查看某服务器的授权列表"
            )

        action = parts[0].lower()

        if action in ["add", "addr"]:
            if len(parts) != 3:
                return "❌ 用法错误: /nginx qq add [服务器名] [QQ号]"
            _, s_name, qq = parts
            if s_name not in self.nginx_servers:
                return f"❌ 未找到被控服务器: {s_name}"
            qq = str(qq)
            acl = self.nginx_acl.setdefault(s_name, [])
            if qq not in acl:
                acl.append(qq)
            self._save_nginx_servers()
            return (
                f"✅ 已为服务器 {s_name} 授权 QQ: {qq}\n"
                f"当前授权用户: {', '.join(acl) if acl else '无'}"
            )

        if action == "rm":
            if len(parts) != 3:
                return "❌ 用法错误: /nginx qq rm [服务器名] [QQ号]"
            _, s_name, qq = parts
            if s_name not in self.nginx_servers:
                return f"❌ 未找到被控服务器: {s_name}"
            qq = str(qq)
            acl = self.nginx_acl.get(s_name, [])
            if qq in acl:
                acl.remove(qq)
                if not acl:
                    self.nginx_acl.pop(s_name, None)
                self._save_nginx_servers()
                return f"✅ 已从服务器 {s_name} 移除 QQ 授权: {qq}"
            return f"ℹ️ QQ {qq} 本来就没有 {s_name} 的编辑权限。"

        if action == "list":
            if len(parts) == 1:
                if not self.nginx_acl:
                    return "📂 当前没有为任何服务器配置 QQ 权限。"
                lines = ["📂 Nginx QQ 权限列表:"]
                for s_name, acl in self.nginx_acl.items():
                    lines.append(
                        f"- {s_name}: {', '.join(acl) if acl else '无'}"
                    )
                return "\n".join(lines)
            if len(parts) == 2:
                s_name = parts[1]
                if s_name not in self.nginx_servers:
                    return f"❌ 未找到被控服务器: {s_name}"
                acl = self.nginx_acl.get(s_name, [])
                return (
                    f"📂 服务器 {s_name} 授权 QQ 列表:\n"
                    f"{', '.join(acl) if acl else '无'}"
                )
            return "❌ 用法错误: /nginx qq list [服务器名]"

        return (
            f"❌ 未知 qq 子命令: {action}\n"
            "可用子命令: add / rm / list"
        )

    async def _test_remote_nginx_server(self, name: str, host: str, port: int):
        """
        测试与远程 NginxAgent 的 WebSocket 连接是否正常：
        1. 尝试建立 ws://host:port 连接
        2. 发送一条 cmd=test 的检测指令
        3. 等待返回并解析 ok 字段
        """
        return await self._call_remote_nginx(name, host, port, "test", {})

    async def _call_remote_nginx(self, server_name: str, host: str, port: int, cmd: str, params: dict):
        """
        调用远程 NginxAgent 执行具体指令:
        - cmd: list / add / rm / test
        - params: 见 nginx_agent.py 中约定
        """
        uri = f"ws://{host}:{port}"
        try:
            async with websockets.connect(uri, open_timeout=5, close_timeout=5) as ws:
                payload = {
                    "type": "nginx_cmd",
                    "cmd": cmd,
                    "params": params or {},
                }
                try:
                    await ws.send(json.dumps(payload, ensure_ascii=False))
                    raw = await asyncio.wait_for(ws.recv(), timeout=30)
                    try:
                        data = json.loads(raw)
                    except json.JSONDecodeError:
                        return False, f"⚠️ 来自 {server_name} ({uri}) 的返回不是合法 JSON。"

                    if data.get("type") != "nginx_result":
                        return False, f"⚠️ 来自 {server_name} ({uri}) 的返回类型异常: {data.get('type')}"

                    ok = bool(data.get("ok"))
                    msg = data.get("message") or ""
                    prefix = f"🎯 目标服务器: {server_name} ({uri})\n"
                    return ok, prefix + msg
                except asyncio.TimeoutError:
                    return False, f"⚠️ 已连接 {uri}，但在等待 {cmd} 响应时超时。"
                except Exception as e:
                    return False, f"⚠️ 已连接 {uri}，发送/接收 {cmd} 指令时出错: {e}"
        except Exception as e:
            return False, f"⚠️ 无法连接到被控服务器 {server_name} ({uri}): {e}"

    async def handle_nginx_server_command(self, parts, user_id: str, msg_type: str):
        """
        处理 /nginx server 子命令：
        /nginx server list
        /nginx server add [名字] [地址]:[端口]
        /nginx server rm [名字]
        """
        # 只有全局管理员可以管理服务器列表
        if not self._is_global_admin(user_id, msg_type):
            return "❌ 只有白名单私聊管理员可以管理被控服务器。"

        if not parts:
            return (
                "🌐 Nginx 被控服务器管理:\n"
                "/nginx server list                 查看所有被控服务器\n"
                "/nginx server add 名字 地址:端口   新增/更新被控服务器\n"
                "  例如: /nginx server add s1 1.2.3.4:9876\n"
                "/nginx server rm 名字              删除被控服务器"
            )

        action = parts[0].lower()

        if action == "list":
            if not self.nginx_servers:
                return "📂 当前没有配置任何被控服务器。"
            lines = ["📂 被控服务器列表:"]
            for name, addr in self.nginx_servers.items():
                lines.append(f"- {name}: {addr}")
            return "\n".join(lines)

        if action == "add":
            if len(parts) != 3:
                return (
                    "❌ 用法错误: /nginx server add [名字] [地址]:[端口]\n"
                    "示例: /nginx server add s1 1.2.3.4:9876"
                )
            _, name, addr = parts
            if not re.fullmatch(r"[a-zA-Z0-9_-]+", name):
                return "❌ 名字只允许使用字母、数字、下划线、短横线。"

            if ":" not in addr:
                return "❌ 地址格式错误，应为 [主机]:[端口]，例如 1.2.3.4:9876 或 agent.example.com:9876"
            host, port_str = addr.rsplit(":", 1)
            host = host.strip()
            if not host:
                return "❌ 主机名不能为空。"
            try:
                port = int(port_str)
                if not (1 <= port <= 65535):
                    return "❌ 端口必须在 1-65535 之间。"
            except ValueError:
                return "❌ 端口必须是数字。"

            value = f"{host}:{port}"
            self.nginx_servers[name] = value
            # 如果还没有默认服务器，则自动设置为第一个
            if not self.nginx_default_server:
                self.nginx_default_server = name
            self._save_nginx_servers()
            ok, test_msg = await self._test_remote_nginx_server(name, host, port)
            prefix = f"✅ 已添加/更新被控服务器: {name} -> {value}\n"
            return prefix + test_msg

        if action == "rm":
            if len(parts) != 2:
                return "❌ 用法错误: /nginx server rm [名字]"
            _, name = parts
            if name not in self.nginx_servers:
                return f"❌ 未找到被控服务器: {name}"
            self.nginx_servers.pop(name, None)
            self._save_nginx_servers()
            return f"✅ 已删除被控服务器: {name}"

        return (
            f"❌ 未知 server 子命令: {action}\n"
            "可用子命令: list / add / rm\n"
            "示例: /nginx server add s1 1.2.3.4:9876"
        )

    async def handle_nginx_command(self, raw_args: str, user_id: str, msg_type: str, group_id: str | None):
        """
        统一处理 /nginx 指令
        /nginx list
        /nginx add [名字] [转发地址] [目标端口] [本地端口]
        /nginx rm [名字]
        /nginx -t                  测试当前 nginx 配置
        /nginx mkdir [文件名]      创建/选择统一配置文件
        /nginx set [服务器名|local] 切换默认被控服务器
        /nginx server ...          管理被控 Nginx 服务器
        """
        args = (raw_args or "").strip()
        default_server = self._get_default_nginx_server()
        is_admin = self._is_global_admin(user_id, msg_type)
        if not args:
            return (
                "🌐 Nginx 管理用法:\n"
                "/nginx list                查看当前所有 stream 配置\n"
                "/nginx add 名字 地址 远端端口 本地端口\n"
                "  例如: /nginx add jpp jpp.yamatu.xyz 46569 10197\n"
                "/nginx rm 名字            删除指定名字的配置\n"
                "/nginx -t                  仅测试 nginx 配置语法 (nginx -t)\n"
                "/nginx mkdir 文件名        在远程创建/选择配置文件\n"
                "/nginx set 服务器名|local  切换默认被控服务器\n"
                "/nginx server ...          管理被控服务器 (list/add/rm)"
            )

        parts = args.split()
        sub = parts[0].lower()

        if sub in ["-h", "--help", "help"]:
            # 显示详细帮助
            return (
                "🌐 Nginx 管理用法:\n"
                "/nginx list                查看当前所有 stream 配置\n"
                "/nginx add 名字 地址 远端端口 本地端口\n"
                "  示例: /nginx add jpp jpp.yamatu.xyz 46569 10197\n"
                "/nginx rm 名字            删除指定名字的配置\n"
                "/nginx -t                  测试 nginx 配置语法 (nginx -t)\n"
                "/nginx mkdir 文件名        在被控服务器上创建/选择配置文件\n"
                "  示例: /nginx mkdir forword\n"
                "/nginx set 服务器名|local  切换默认被控服务器\n"
                "  示例: /nginx set jpix\n"
                "/nginx qq add 服务器名 QQ  授权 QQ 编辑指定服务器配置\n"
                "/nginx qq rm 服务器名 QQ   撤销 QQ 的编辑权限\n"
                "/nginx server ...          管理被控服务器 (list/add/rm)\n"
                "  示例: /nginx server add jpix 1.2.3.4:10190"
            )

        if sub == "list":
            # 若存在被控服务器则优先操作远程，否则操作本机
            if default_server:
                s_name, s_host, s_port = default_server
                ok, msg = await self._call_remote_nginx(s_name, s_host, s_port, "list", {})
                return msg
            return await self.nginx_list_configs()
        elif sub in ["-t", "test", "check"]:
            if default_server:
                s_name, s_host, s_port = default_server
                ok, msg = await self._call_remote_nginx(s_name, s_host, s_port, "test", {})
                return msg
            return await self.nginx_test_config()
        elif sub == "mkdir":
            if not default_server:
                return "❌ 当前未配置任何被控服务器，请先使用 /nginx server add 添加。"
            if len(parts) != 2:
                return "❌ 用法错误: /nginx mkdir [文件名]"
            conf_name = parts[1]
            s_name, s_host, s_port = default_server
            # 非全局管理员需要具备该服务器的编辑权限
            if not is_admin and user_id not in self.nginx_acl.get(s_name, []):
                return (
                    f"❌ 你没有权限修改服务器 {s_name} 的配置，请联系管理员使用 "
                    "/nginx qq add 授权。"
                )
            # 记录每个服务器使用的配置文件
            self.nginx_server_confs[s_name] = conf_name
            self._save_nginx_servers()
            ok, msg = await self._call_remote_nginx(
                s_name, s_host, s_port, "mkdir", {"conf": conf_name}
            )
            return msg
        elif sub == "set":
            # 仅全局管理员可以切换默认服务器
            if not is_admin:
                return "❌ 只有白名单私聊管理员可以使用 /nginx set 切换默认服务器。"
            if len(parts) != 2:
                return "❌ 用法错误: /nginx set [服务器名|local]"
            target = parts[1]
            if target.lower() == "local":
                self.nginx_default_server = None
                self._save_nginx_servers()
                return "✅ 已切换到本机 Nginx，不再使用远程被控服务器。"
            if target not in self.nginx_servers:
                return f"❌ 未找到被控服务器: {target}"
            self.nginx_default_server = target
            self._save_nginx_servers()
            addr = self.nginx_servers[target]
            conf_name = self.nginx_server_confs.get(target)
            extra = (
                f"，当前配置文件: {conf_name}.conf"
                if conf_name
                else "，尚未选择配置文件，请先 /nginx mkdir [文件名]"
            )
            return f"✅ 默认被控服务器已切换为: {target} -> {addr}{extra}"
        elif sub == "server":
            return await self.handle_nginx_server_command(parts[1:], user_id, msg_type)
        elif sub == "qq":
            # 权限管理，仅全局管理员可操作
            return await self.handle_nginx_qq_command(parts[1:], user_id, msg_type)
        elif sub == "add":
            if len(parts) != 5:
                return (
                    "❌ 用法错误: /nginx add [名字] [转发地址] [目标端口] [本地端口]\n"
                    "示例: /nginx add jpp jpp.yamatu.xyz 46569 10197"
                )
            _, name, host, t_port, l_port = parts
            if default_server:
                s_name, s_host, s_port = default_server
                # 非全局管理员需要具备该服务器的编辑权限
                if not is_admin and user_id not in self.nginx_acl.get(s_name, []):
                    return (
                        f"❌ 你没有权限修改服务器 {s_name} 的配置，请联系管理员使用 "
                        "/nginx qq add 授权。"
                    )
                conf_name = self.nginx_server_confs.get(s_name)
                if not conf_name:
                    return (
                        f"❌ 当前默认服务器 {s_name} 尚未选择配置文件，"
                        "请先使用 /nginx mkdir [文件名]"
                    )
                params = {
                    "conf": conf_name,
                    "name": name,
                    "target_host": host,
                    "target_port": t_port,
                    "listen_port": l_port,
                }
                ok, msg = await self._call_remote_nginx(s_name, s_host, s_port, "add", params)
                return msg
            return await self.nginx_add_config(name, host, t_port, l_port)
        elif sub == "rm":
            if len(parts) != 2:
                return "❌ 用法错误: /nginx rm [名字]"
            _, name = parts
            if default_server:
                s_name, s_host, s_port = default_server
                if not is_admin and user_id not in self.nginx_acl.get(s_name, []):
                    return (
                        f"❌ 你没有权限修改服务器 {s_name} 的配置，请联系管理员使用 "
                        "/nginx qq add 授权。"
                    )
                conf_name = self.nginx_server_confs.get(s_name)
                if not conf_name:
                    return (
                        f"❌ 当前默认服务器 {s_name} 尚未选择配置文件，"
                        "请先使用 /nginx mkdir [文件名]"
                    )
                params = {"conf": conf_name, "name": name}
                ok, msg = await self._call_remote_nginx(s_name, s_host, s_port, "rm", params)
                return msg
            return await self.nginx_remove_config(name)
        else:
            return (
                f"❌ 未知子命令: {sub}\n"
                "可用子命令: list / add / rm / -t / server\n"
                "示例: /nginx add jpp jpp.yamatu.xyz 46569 10197\n"
                "      /nginx -t\n"
                "      /nginx server list"
            )

    async def ping_via_proxy(self, raw_input):
        """
        [新增] 代理 Ping 功能
        1. 正则提取主机名
        2. 强制走代理 Session 请求
        3. 计算 HTTP 响应延迟
        """
        if not raw_input: return "❌ 请输入域名或IP"
        
        # 提取主机名 (例如: https://google.com/path -> google.com)
        match = self.re_host_extract.search(raw_input.strip())
        if not match:
            return f"❌ 无法解析域名: {raw_input}"
            
        host = match.group('host')
        
        # 构建测试 URL (默认 HTTPS，如果失败会自动回退吗？这里只测试连通性)
        target_url = f"https://{host}"
        
        start_time = time.perf_counter()
        try:
            # 使用 HEAD 请求，减少流量，只看连接耗时
            # 使用 proxy_session 确保走 SOCKS5
            async with self.proxy_session.get(target_url, timeout=10) as resp:
                latency = (time.perf_counter() - start_time) * 1000
                status_icon = "✅" if resp.status < 400 else "⚠️"
                return f"{status_icon} Proxy Ping\n🎯 目标: {host}\n📶 延迟: {latency:.2f}ms\n🔢 状态码: {resp.status}"
        except asyncio.TimeoutError:
            return f"❌ Proxy Ping 超时\n🎯 目标: {host}\n⏳ 超过10s无响应"
        except Exception as e:
            return f"❌ Proxy Ping 失败\n🎯 目标: {host}\n⚠️ 错误: {str(e)}"
    
    async def get_weather(self, city_name):
        try:
            async with self.session.get(
                "https://restapi.amap.com/v3/geocode/geo",
                params={"key": AMAP_API_KEY, "address": city_name}
            ) as resp:
                data = await resp.json()
                if not data.get("geocodes"): return "❌ 未找到该地区"
                adcode = data["geocodes"][0]["adcode"]
            
            async with self.session.get(
                "https://restapi.amap.com/v3/weather/weatherInfo",
                params={"key": AMAP_API_KEY, "city": adcode, "extensions": "base"}
            ) as resp:
                data = await resp.json()
                if data.get("lives"):
                    w = data["lives"][0]
                    return f"🌤️ {w['province']} {w['city']} 天气\n{w['weather']} {w['temperature']}℃\n{w['winddirection']}风 {w['windpower']}级\n湿度: {w['humidity']}%"
                return "❌ 天气查询失败"
        except Exception as e:
            return f"❌ API错误: {e}"

    async def get_epic_free_games(self):
        try:
            url = "https://store-site-backend-static-ipv4.ak.epicgames.com/freeGamesPromotions?locale=zh-CN&country=CN&allowCountries=CN"
            async with self.session.get(url) as resp:
                data = await resp.json()
                games = data.get("data", {}).get("Catalog", {}).get("searchStore", {}).get("elements", [])
                free_list = []
                for game in games:
                    promotions = game.get("promotions")
                    if not promotions: continue
                    is_free = False
                    offers = promotions.get("promotionalOffers", [])
                    if not offers and promotions.get("upcomingPromotionalOffers"):
                        offers = promotions.get("upcomingPromotionalOffers", [])
                    for promo in offers:
                        for offer in promo.get("promotionalOffers", []):
                            if offer.get("discountSetting", {}).get("discountPercentage") == 0:
                                is_free = True; break
                    if is_free:
                        title = game.get("title")
                        slug = game.get("productSlug") or game.get("urlSlug")
                        link = f"https://store.epicgames.com/zh-CN/p/{slug}" if slug else ""
                        free_list.append(f"🎮 {title}\n🔗 {link}")
                if not free_list: return "🎮 当前没有免费游戏"
                return "🎮 Epic 喜加一:\n\n" + "\n".join(free_list[:3])
        except Exception as e:
            return f"❌ Epic查询失败: {e}"

    async def get_bilibili_hot_search(self):
        try:
            url = "https://app.bilibili.com/x/v2/search/trending/ranking"
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Referer": "https://www.bilibili.com/"
            }
            async with self.session.get(url, params={"limit": 15}, headers=headers) as resp:
                if resp.status != 200: return f"❌ 获取失败 (Code {resp.status})"
                data = await resp.json()
                if data["code"] == 0:
                    items = data["data"]["list"]
                    res = ["🔥 B站热搜榜:"]
                    for i, item in enumerate(items, 1):
                        res.append(f"{i}. {item['show_name']}")
                    return "\n".join(res)
                return "❌ 获取失败"
        except Exception as e:
            return f"❌ 错误: {e}"

    async def parse_bilibili_bv(self, bvid):
        try:
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
            async with self.session.get(
                BILIBILI_API_BASE, params={"bvid": bvid}, headers=headers
            ) as resp:
                data = await resp.json()
                if data["code"] != 0: return None, f"❌ 视频失效: {data.get('message')}"
                info = data["data"]
                stat = info["stat"]
                res = (
                    f"📺 {info['title']}\n"
                    f"👤 UP: {info['owner']['name']}\n"
                    f"📊 播放: {stat['view']}  👍 {stat['like']}  💰 {stat['coin']}\n"
                    f"🔗 https://www.bilibili.com/video/{bvid}"
                )
                return info['pic'], res
        except Exception as e:
            return None, f"❌ 解析错误: {e}"

    async def _fetch_image_base64(self, url, use_proxy=False):
        session = self.proxy_session if use_proxy else self.session
        try:
            async with session.get(url, timeout=15) as resp:
                if resp.status == 200:
                    content = await resp.read()
                    b64 = base64.b64encode(content).decode('ascii')
                    return f"base64://{b64}"
        except:
            return None
        return None

    async def parse_youtube_video(self, video_id):
        try:
            url = f"https://www.youtube.com/watch?v={video_id}"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
                'Accept-Language': 'zh-CN,zh;q=0.9'
            }
            async with self.proxy_session.get(url, headers=headers, timeout=30) as resp:
                text = await resp.text()
                sel = Selector(text=text)
                title = sel.css('title::text').get() or "Unknown"
                title = title.replace(" - YouTube", "")
                cover_url = f"https://i.ytimg.com/vi/{video_id}/mqdefault.jpg"
                b64 = await self._fetch_image_base64(cover_url, use_proxy=True)
                res = f"🎬 YouTube 视频\n📺 标题: {title}\n🔗 链接: {url}"
                return b64 or cover_url, res
        except Exception as e:
            return None, f"❌ 解析异常: {e}"

    # ============ AI 调用逻辑 ============
    async def call_gpt_api(self, messages):
        try:
            if HAS_CURL_CFFI:
                def _req():
                    return curl_requests.post(
                        f"{GPT_API_BASE}/chat/completions",
                        headers={"Authorization": f"Bearer {GPT_API_KEY}"},
                        json={"model": GPT_MODEL, "messages": messages, "max_tokens": 4000},
                        impersonate="chrome120", timeout=120
                    )
                resp = await asyncio.to_thread(_req)
                if resp.status_code == 200:
                    return resp.json()["choices"][0]["message"]["content"]
                return f"GPT API Error: {resp.status_code}"
            else:
                resp = await self.gpt_client.chat.completions.create(
                    model=GPT_MODEL, messages=messages, max_tokens=4000
                )
                return resp.choices[0].message.content
        except Exception as e:
            return f"❌ GPT 调用失败: {e}"

    async def call_grok_api(self, messages):
        try:
            headers = {"Authorization": f"Bearer {GROK_API_KEY}"}
            payload = {"model": GROK_MODEL, "messages": messages, "max_tokens": 2000}
            async with self.proxy_session.post(
                f"{GROK_API_BASE}/chat/completions", headers=headers, json=payload
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data["choices"][0]["message"]["content"]
                return f"Grok Error: {resp.status}"
        except Exception as e:
            return f"❌ Grok Error: {e}"

    # ============ 业务逻辑 ============
    async def process_single_message(self, websocket, payload):
        try:
            post_type = payload.get("post_type")
            if post_type != "message": return

            msg_type = payload.get("message_type")
            user_id = str(payload.get("user_id"))
            group_id = str(payload.get("group_id")) if msg_type == "group" else None
            raw_msg = payload.get("raw_message", "")
            self_id = str(payload.get("self_id"))

            if msg_type == "private" and user_id not in ALLOWED_QQ_USERS: return
            if msg_type == "group" and ALLOWED_GROUP_IDS and group_id not in ALLOWED_GROUP_IDS: return

            clean_msg = self._strip_cq_codes(raw_msg)
            is_at_me = False
            if msg_type == "group" and f"[CQ:at,qq={self_id}]" in raw_msg:
                is_at_me = True

            response_text = None
            response_img = None

            # 小程序消息 (B站)
            if raw_msg.startswith("[CQ:json,data=") and ('哔哩哔哩' in raw_msg or 'b23.tv' in raw_msg):
                 if msg_type != "group" or self.is_group_bv_enabled(group_id):
                     try:
                         url_match = re.search(r'(http[^\"]+)', raw_msg)
                         if url_match:
                            raw_url = url_match.group(1)
                            bv_match = self.re_bv.search(raw_url)
                            if bv_match:
                                response_img, response_text = await self.parse_bilibili_bv(bv_match.group())
                            elif 'b23.tv' in raw_url:
                                try:
                                    async with self.session.get(raw_url, allow_redirects=False) as r:
                                        loc = r.headers.get('Location', '')
                                        bv_match = self.re_bv.search(loc)
                                        if bv_match:
                                            response_img, response_text = await self.parse_bilibili_bv(bv_match.group())
                                except: pass
                     except: pass

            # 指令处理
            if not response_text:
                if clean_msg.startswith("/help"):
                    response_text = (
                        "🤖 帮助:\n"
                        "/ct [问题]           - 问答\n"
                        "/ping [域名]         - 代理测速\n"
                        "/nginx ...           - 管理 Nginx stream/远程转发\n"
                        "  /nginx list        - 列出当前配置\n"
                        "  /nginx add ...     - 新增/更新转发\n"
                        "  /nginx rm 名字     - 删除转发\n"
                        "  /nginx -t          - 测试 nginx 配置语法\n"
                        "  /nginx mkdir 名称  - 初始化/选择配置文件\n"
                        "  /nginx set 名称    - 切换默认被控服务器\n"
                        "  /nginx server ...  - 管理被控服务器\n"
                        "/set [模型]          - 个人模型\n"
                        "/setall [模型]       - 群模型\n"
                        "/clear               - 清除记忆\n"
                        "/天气 [城市]\n"
                        "/rs                  - B站热搜\n"
                        "/epic                - Epic 喜加一\n"
                        "/bv [BV号/on/off]"
                    )
                
                # 【新增】Ping 功能
                elif clean_msg.startswith("/ping "):
                    response_text = await self.ping_via_proxy(clean_msg[6:].strip())

                elif clean_msg.startswith("/nginx"):
                    # 允许 "/nginx" 或 "/nginx xxx" 两种形式
                    response_text = await self.handle_nginx_command(
                        clean_msg[len("/nginx"):], user_id, msg_type, group_id
                    )

                elif clean_msg.startswith("/天气 "):
                    response_text = await self.get_weather(clean_msg[4:].strip())
                elif clean_msg.startswith("/rs"):
                    response_text = await self.get_bilibili_hot_search()
                elif clean_msg.startswith("/epic"):
                    response_text = await self.get_epic_free_games()
                
                elif clean_msg.startswith("/set "):
                    m = clean_msg[5:].strip().lower()
                    if self.set_user_model(user_id, m): response_text = f"✅ 个人模型: {m}"
                    else: response_text = "❌ 未知模型"
                
                elif clean_msg.startswith("/setall ") and msg_type == "group":
                    m = clean_msg[8:].strip().lower()
                    if m in ["gpt", "claude", "grok"]:
                        self.set_group_default_model(group_id, m)
                        response_text = f"✅ 群默认模型: {m}"
                    else: response_text = "❌ 未知模型"
                
                elif clean_msg == "/clear":
                    if self.clear_user_memory(user_id): response_text = "🧹 记忆已清除"
                    else: response_text = "ℹ️ 无记忆可清除"

                elif clean_msg.startswith("/bv "):
                    arg = clean_msg[4:].strip()
                    if arg == "on":
                        self.set_group_bv_enabled(group_id, True); response_text = "✅ BV解析开启"
                    elif arg == "off":
                        self.set_group_bv_enabled(group_id, False); response_text = "🚫 BV解析关闭"
                    else:
                        response_img, response_text = await self.parse_bilibili_bv(arg)

            # 链接识别
            if not response_text and (msg_type == "private" or self.is_group_bv_enabled(group_id)):
                bv_match = self.re_bv.search(clean_msg)
                if bv_match: response_img, response_text = await self.parse_bilibili_bv(bv_match.group())

            if not response_text:
                for p in self.re_youtube:
                    yt = p.search(clean_msg)
                    if yt:
                        response_img, response_text = await self.parse_youtube_video(yt.group(1))
                        break

            # AI 对话
            should_chat = False
            prompt = ""
            temp_model = None

            if not response_text:
                if clean_msg.startswith("/ct "):
                    should_chat = True; prompt = clean_msg[4:].strip()
                elif clean_msg.startswith("/grok "):
                    should_chat = True; prompt = clean_msg[6:].strip(); temp_model = "grok"
                elif msg_type == "private":
                    should_chat = True; prompt = clean_msg
                elif is_at_me:
                    should_chat = True; prompt = re.sub(r'@\d+\s*', '', clean_msg).strip()

            if should_chat and prompt:
                model_key = temp_model or self.get_user_model(user_id, group_id)
                system_prompt = self.model_prompts.get(model_key, "")
                
                history = self.user_memory.get(user_id, {}).get("history", [])
                msgs = [{"role": "system", "content": system_prompt}]
                msgs.extend([m for m in history if m.get("content")])
                msgs.append({"role": "user", "content": prompt})

                ans = ""
                if model_key == "claude":
                    try:
                        chat_msgs = [{"role": m["role"], "content": m["content"]} for m in msgs if m["role"] != "system"]
                        resp = await self.claude_client.messages.create(
                            model=CLAUDE_MODEL, max_tokens=2000, system=msgs[0]["content"], messages=chat_msgs
                        )
                        ans = resp.content[0].text
                    except Exception as e: ans = f"Error: {e}"
                elif model_key == "grok": ans = await self.call_grok_api(msgs)
                else: ans = await self.call_gpt_api(msgs)

                self._append_history(user_id, "user", prompt)
                self._append_history(user_id, "assistant", ans)
                response_text = f"🤖 [{model_key}]\n{ans}"

            if response_text:
                final_msg = response_text
                if response_img: final_msg = f"[CQ:image,file={response_img}]\n{response_text}"
                await self.send_long_text(websocket, msg_type, int(user_id) if msg_type == "private" else int(group_id), final_msg)

        except Exception as e:
            logger.error(f"Msg Error: {e}", exc_info=True)

    async def client_handler(self, websocket):
        logger.info("New Client Connected")
        try:
            async for message in websocket:
                try:
                    data = json.loads(message)
                    asyncio.create_task(self.process_single_message(websocket, data))
                except json.JSONDecodeError: pass
        except websockets.ConnectionClosed:
            logger.info("Client Disconnected")

    async def run(self):
        self._init_network_clients()
        asyncio.create_task(self._background_save_task())
        logger.info("🚀 Server Started on 0.0.0.0:8765")
        async with websockets.serve(self.client_handler, "0.0.0.0", 8765):
            await asyncio.Future()

if __name__ == "__main__":
    bot = QQBotServer()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        asyncio.run(bot.cleanup())
