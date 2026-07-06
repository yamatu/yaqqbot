package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	_ "image/gif"
	"image/jpeg"
	"image/png"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unicode"

	"github.com/gorilla/websocket"

	"qq_client/internal/codexcli"
	"qq_client/internal/openaiutil"
)

// 本文件是对 qqbot_server.py 的 Golang 重写版本。
// 目标尽量保持功能对等，但做了少量工程化调整（例如配置与密钥从环境变量读取）。

// ============ 配置区域 ============

const (
	// 持久化文件
	memoryFile         = "user_memory.json"
	groupSettingsKey   = "__group_settings__"
	nginxServersFile   = "nginx_servers.json"
	steamWatchFile     = "steam_watch.json"
	messageChunkSize   = 4000
	nginxStreamConfDir = "/etc/nginx/stream.conf.d"

	// 配置文件路径
	configFilePath = "configs/config.json"

	// WebSocket 监听地址
	wsListenAddr = "0.0.0.0:8765"
)

// QQ 私聊白名单
var allowedQQUsers = []string{"984346643", "836644146", "3541975032"}

// 群聊白名单
var allowedGroupIDs = []string{"1021625874", "421953860", "827500600", "1039488471"}

// API 配置（密钥从环境变量读取，避免硬编码）
var (
	claudeAPIKey  = os.Getenv("CLAUDE_API_KEY")
	claudeAPIBase = envOrDefault("CLAUDE_API_BASE", "https://agentrouter.org")
	claudeModel   = envOrDefault("CLAUDE_MODEL", "claude-sonnet-4-5-20250929")

	gptAPIKey = os.Getenv("GPT_API_KEY")
	// OpenAI 官方接口通常为 https://api.openai.com/v1
	// 注意：不要填写成 https://api.openai.com/v1/codex（这是 Codex CLI 专用前缀）
	gptAPIBase = envOrDefault("GPT_API_BASE", "https://api.openai.com/v1")
	gptModel   = envOrDefault("GPT_MODEL", "gpt-5.1")

	grokAPIKey  = os.Getenv("GROK_API_KEY")
	grokAPIBase = envOrDefault("GROK_API_BASE", "https://happyapi.org/v1")
	grokModel   = envOrDefault("GROK_MODEL", "grok-3")

	// Gemini API（用于图片生成）
	geminiAPIKey     = os.Getenv("GEMINI_API_KEY")
	geminiAPIBase    = envOrDefault("GEMINI_API_BASE", "https://generativelanguage.googleapis.com/v1beta")
	geminiImageModel = envOrDefault("GEMINI_IMAGE_MODEL", "gemini-2.5-flash-image")

	deepSeekAPIKey  = os.Getenv("DEEPSEEK_API_KEY")
	deepSeekAPIBase = envOrDefault("DEEPSEEK_API_BASE", "https://api.deepseek.com")
	deepSeekModel   = envOrDefault("DEEPSEEK_MODEL", "deepseek-v4-flash")

	steamAPIKey          = os.Getenv("STEAM_API_KEY")
	steamAPIBase         = envOrDefault("STEAM_API_BASE", "https://api.steampowered.com")
	steamAPIKeyDomain    = envOrDefault("STEAM_API_KEY_DOMAIN", "book.yamatu.xyz")
	steamMonitorGroups   = parseCSVEnv("STEAM_MONITOR_GROUPS")
	steamPollInterval    = envDurationOrDefault("STEAM_POLL_INTERVAL", 60*time.Second)
	longForwardThreshold = envIntOrDefault("LONG_FORWARD_THRESHOLD", 3000)
	maxContextChars      = envIntOrDefault("MAX_CONTEXT_CHARS", 24000)

	amapAPIKey      = os.Getenv("AMAP_API_KEY")
	bilibiliAPIBase = envOrDefault("BILIBILI_API_BASE", "https://api.bilibili.com/x/web-interface/view")

	// SOCKS5/HTTP 代理（例如 socks5://127.0.0.1:41457 或 http://127.0.0.1:1080）
	socks5Proxy = os.Getenv("SOCKS5_PROXY")
)

// 提示词文件
var promptFiles = map[string]string{
	"grok":     "grok.txt",
	"gpt":      "gpt.txt",
	"claude":   "claude.txt",
	"deepseek": "deepseek.txt",
}

// ============ 数据结构 ============

type historyItem struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type userEvent struct {
	Time    string `json:"time"`
	Type    string `json:"type"`
	GroupID string `json:"group_id,omitempty"`
	Detail  string `json:"detail"`
}

type userProfile struct {
	History []historyItem `json:"history"`
	Model   string        `json:"model"`
	Events  []userEvent   `json:"events,omitempty"`
}

type groupSettings struct {
	BvEnabled    map[string]bool   `json:"bv_enabled"`
	ModelDefault map[string]string `json:"model_default"`
}

// 内存文件顶层结构：用户ID -> userProfile，外加 __group_settings__。

type memorySnapshot struct {
	Users         map[string]*userProfile
	GroupSettings groupSettings
}

type nginxConfigFile struct {
	Default string              `json:"__default__"`
	Conf    map[string]string   `json:"__conf__"`
	ACL     map[string][]string `json:"__acl__"`
	Servers map[string]string   `json:"-"` // 其余顶层键为服务器 name->addr
}

// QQ 消息载体（只保留本项目用到的字段）
type cqMessage struct {
	PostType    string `json:"post_type"`
	MessageType string `json:"message_type"`
	NoticeType  string `json:"notice_type"`
	SubType     string `json:"sub_type"`
	RequestType string `json:"request_type"`
	UserID      int64  `json:"user_id"`
	TargetID    int64  `json:"target_id"`
	OperatorID  int64  `json:"operator_id"`
	GroupID     int64  `json:"group_id"`
	RawMessage  string `json:"raw_message"`
	SelfID      int64  `json:"self_id"`
	MessageID   int64  `json:"message_id"`
	Comment     string `json:"comment"`
}

// WebSocket 客户端封装，保证写操作串行。
type wsClient struct {
	conn    *websocket.Conn
	writeMu sync.Mutex

	pendingMu sync.Mutex
	pending   map[string]chan []byte
}

func (c *wsClient) WriteJSON(v any) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.conn.WriteJSON(v)
}

var wsEchoSeq uint64

type oneBotActionResp struct {
	Status  string          `json:"status"`
	Retcode int             `json:"retcode"`
	Data    json.RawMessage `json:"data"`
	Msg     string          `json:"msg"`
	Wording string          `json:"wording"`
	Echo    string          `json:"echo"`
}

type steamWatchEntry struct {
	SteamID      string `json:"steam_id"`
	Name         string `json:"name"`
	LastState    int    `json:"last_state"`
	LastGameID   string `json:"last_game_id,omitempty"`
	LastGameName string `json:"last_game_name,omitempty"`
	GameStarted  string `json:"game_started,omitempty"`
	UpdatedAt    string `json:"updated_at,omitempty"`
}

type steamWatchState struct {
	Watched map[string]*steamWatchEntry `json:"watched"`
}

func (c *wsClient) Call(ctx context.Context, action string, params map[string]any) (json.RawMessage, error) {
	echo := fmt.Sprintf("e-%d", atomic.AddUint64(&wsEchoSeq, 1))
	ch := make(chan []byte, 1)
	c.pendingMu.Lock()
	if c.pending == nil {
		c.pending = make(map[string]chan []byte)
	}
	c.pending[echo] = ch
	c.pendingMu.Unlock()
	defer func() {
		c.pendingMu.Lock()
		delete(c.pending, echo)
		c.pendingMu.Unlock()
	}()

	req := map[string]any{
		"action": action,
		"params": params,
		"echo":   echo,
	}
	if err := c.WriteJSON(req); err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case b := <-ch:
		var resp oneBotActionResp
		if err := json.Unmarshal(b, &resp); err != nil {
			return nil, err
		}
		if resp.Status != "ok" || resp.Retcode != 0 {
			msg := strings.TrimSpace(resp.Wording)
			if msg == "" {
				msg = strings.TrimSpace(resp.Msg)
			}
			if msg == "" {
				msg = fmt.Sprintf("status=%s retcode=%d", resp.Status, resp.Retcode)
			}
			return nil, errors.New(msg)
		}
		return resp.Data, nil
	}
}

func (c *wsClient) fulfillEcho(echo string, payload []byte) bool {
	c.pendingMu.Lock()
	ch := c.pending[echo]
	c.pendingMu.Unlock()
	if ch == nil {
		return false
	}
	select {
	case ch <- payload:
	default:
	}
	return true
}

// QQBot 主结构
type qqBotServer struct {
	// 关闭控制
	shutdown chan struct{}
	wg       sync.WaitGroup

	// 日志
	logger *log.Logger

	// 内存与模型
	memoryMu      sync.RWMutex
	users         map[string]*userProfile
	groupSettings groupSettings
	memoryDirty   bool
	modelPrompts  map[string]string
	defaultModel  string

	// Nginx 被控服务器
	nginxMu          sync.RWMutex
	nginxServers     map[string]string // name -> host:port
	nginxDefault     string            // 当前默认服务器名（为空表示本机）
	nginxServerConfs map[string]string // name -> conf filename
	nginxACL         map[string][]string

	// Steam 监控与已连接 OneBot 客户端
	steamMu    sync.RWMutex
	steamWatch map[string]*steamWatchEntry
	clientsMu  sync.RWMutex
	clients    map[*wsClient]struct{}

	// HTTP 客户端
	httpClient  *http.Client
	proxyClient *http.Client

	// 正则
	reCQ            *regexp.Regexp
	reCQImage       *regexp.Regexp
	reCQReply       *regexp.Regexp
	reBV            *regexp.Regexp
	reBilibiliURL   *regexp.Regexp
	reBilibiliShort *regexp.Regexp
	reYouTube       []*regexp.Regexp
	reHostExtract   *regexp.Regexp

	// 白名单快速查找
	allowedUsers  map[string]struct{}
	allowedGroups map[string]struct{}
}

// ============ 工具函数 ============

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envBoolOrDefault(key string, def bool) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	switch strings.ToLower(v) {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return def
	}
}

func envIntOrDefault(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func envDurationOrDefault(key string, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	if d, err := time.ParseDuration(v); err == nil && d > 0 {
		return d
	}
	if n, err := strconv.Atoi(v); err == nil && n > 0 {
		return time.Duration(n) * time.Second
	}
	return def
}

func parseCSVEnv(key string) []string {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func stripJSONComments(input []byte) []byte {
	out := make([]byte, 0, len(input))
	inString := false
	escaped := false
	for i := 0; i < len(input); i++ {
		ch := input[i]
		if inString {
			out = append(out, ch)
			if escaped {
				escaped = false
				continue
			}
			if ch == '\\' {
				escaped = true
				continue
			}
			if ch == '"' {
				inString = false
			}
			continue
		}
		if ch == '"' {
			inString = true
			out = append(out, ch)
			continue
		}
		if ch == '/' && i+1 < len(input) {
			next := input[i+1]
			if next == '/' {
				i += 2
				for i < len(input) && input[i] != '\n' && input[i] != '\r' {
					i++
				}
				if i < len(input) {
					out = append(out, input[i])
				}
				continue
			}
			if next == '*' {
				i += 2
				for i+1 < len(input) && !(input[i] == '*' && input[i+1] == '/') {
					i++
				}
				i++
				continue
			}
		}
		out = append(out, ch)
	}
	return out
}

// fileConfig 对应 configs/config.json，用来从文件加载 API Key 等敏感配置。
type fileConfig struct {
	ClaudeAPIKey         string   `json:"claude_api_key"`
	ClaudeAPIBase        string   `json:"claude_api_base"`
	ClaudeModel          string   `json:"claude_model"`
	GPTAPIKey            string   `json:"gpt_api_key"`
	GPTAPIBase           string   `json:"gpt_api_base"`
	GPTModel             string   `json:"gpt_model"`
	GrokAPIKey           string   `json:"grok_api_key"`
	GrokAPIBase          string   `json:"grok_api_base"`
	GrokModel            string   `json:"grok_model"`
	GeminiAPIKey         string   `json:"gemini_api_key"`
	GeminiAPIBase        string   `json:"gemini_api_base"`
	GeminiImageModel     string   `json:"gemini_image_model"`
	DeepSeekAPIKey       string   `json:"deepseek_api_key"`
	DeepSeekAPIBase      string   `json:"deepseek_api_base"`
	DeepSeekModel        string   `json:"deepseek_model"`
	SteamAPIKey          string   `json:"steam_api_key"`
	SteamAPIBase         string   `json:"steam_api_base"`
	SteamAPIKeyDomain    string   `json:"steam_api_key_domain"`
	SteamMonitorGroups   []string `json:"steam_monitor_groups"`
	SteamPollInterval    string   `json:"steam_poll_interval"`
	LongForwardThreshold int      `json:"long_forward_threshold"`
	MaxContextChars      int      `json:"max_context_chars"`
	AMapAPIKey           string   `json:"amap_api_key"`
	BilibiliAPIBase      string   `json:"bilibili_api_base"`
	Socks5Proxy          string   `json:"socks5_proxy"`
}

// loadConfigFromFile 从 JSON 配置文件加载配置，并覆盖默认的环境变量值。
// 如果文件不存在，则保持原有 env 配置不变。
func loadConfigFromFile(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("读取配置文件失败: %v", err)
		}
		return
	}
	var cfg fileConfig
	if err := json.Unmarshal(stripJSONComments(data), &cfg); err != nil {
		log.Printf("解析配置文件失败: %v", err)
		return
	}

	// 仅当配置文件中对应字段非空时，才覆盖默认值
	if cfg.ClaudeAPIKey != "" {
		claudeAPIKey = cfg.ClaudeAPIKey
	}
	if cfg.ClaudeAPIBase != "" {
		claudeAPIBase = cfg.ClaudeAPIBase
	}
	if cfg.ClaudeModel != "" {
		claudeModel = cfg.ClaudeModel
	}
	if cfg.GPTAPIKey != "" {
		gptAPIKey = cfg.GPTAPIKey
	}
	if cfg.GPTAPIBase != "" {
		gptAPIBase = cfg.GPTAPIBase
	}
	if cfg.GPTModel != "" {
		gptModel = cfg.GPTModel
	}
	if cfg.GrokAPIKey != "" {
		grokAPIKey = cfg.GrokAPIKey
	}
	if cfg.GrokAPIBase != "" {
		grokAPIBase = cfg.GrokAPIBase
	}
	if cfg.GrokModel != "" {
		grokModel = cfg.GrokModel
	}
	if cfg.GeminiAPIKey != "" {
		geminiAPIKey = cfg.GeminiAPIKey
	}
	if cfg.GeminiAPIBase != "" {
		geminiAPIBase = cfg.GeminiAPIBase
	}
	if cfg.GeminiImageModel != "" {
		geminiImageModel = cfg.GeminiImageModel
	}
	if cfg.DeepSeekAPIKey != "" {
		deepSeekAPIKey = cfg.DeepSeekAPIKey
	}
	if cfg.DeepSeekAPIBase != "" {
		deepSeekAPIBase = cfg.DeepSeekAPIBase
	}
	if cfg.DeepSeekModel != "" {
		deepSeekModel = cfg.DeepSeekModel
	}
	if cfg.SteamAPIKey != "" {
		steamAPIKey = cfg.SteamAPIKey
	}
	if cfg.SteamAPIBase != "" {
		steamAPIBase = cfg.SteamAPIBase
	}
	if cfg.SteamAPIKeyDomain != "" {
		steamAPIKeyDomain = cfg.SteamAPIKeyDomain
	}
	if len(cfg.SteamMonitorGroups) > 0 {
		steamMonitorGroups = cfg.SteamMonitorGroups
	}
	if cfg.SteamPollInterval != "" {
		if d, err := time.ParseDuration(cfg.SteamPollInterval); err == nil && d > 0 {
			steamPollInterval = d
		} else if n, err := strconv.Atoi(cfg.SteamPollInterval); err == nil && n > 0 {
			steamPollInterval = time.Duration(n) * time.Second
		}
	}
	if cfg.LongForwardThreshold > 0 {
		longForwardThreshold = cfg.LongForwardThreshold
	}
	if cfg.MaxContextChars > 0 {
		maxContextChars = cfg.MaxContextChars
	}
	if cfg.AMapAPIKey != "" {
		amapAPIKey = cfg.AMapAPIKey
	}
	if cfg.BilibiliAPIBase != "" {
		bilibiliAPIBase = cfg.BilibiliAPIBase
	}
	if cfg.Socks5Proxy != "" {
		socks5Proxy = cfg.Socks5Proxy
	}
}

func newLogger() *log.Logger {
	logFile, err := os.OpenFile("qqbot.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		// 退化为只输出到控制台
		return log.New(os.Stdout, "", log.LstdFlags)
	}
	mw := io.MultiWriter(os.Stdout, logFile)
	return log.New(mw, "", log.LstdFlags)
}

func newHTTPClient() *http.Client {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: false, // 尽量使用 IPv4
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	return &http.Client{
		Transport: transport,
		Timeout:   60 * time.Second,
	}
}

// 目前 proxyClient 仅支持 HTTP/HTTPS 代理；如果 socks5Proxy 是 socks5:// 开头，会忽略代理设置。
func newProxyHTTPClient() *http.Client {
	if socks5Proxy == "" {
		return newHTTPClient()
	}
	u, err := url.Parse(socks5Proxy)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		// 非 HTTP 代理暂不支持，退化为普通客户端
		return newHTTPClient()
	}
	transport := &http.Transport{
		Proxy: http.ProxyURL(u),
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: false,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	return &http.Client{
		Transport: transport,
		Timeout:   60 * time.Second,
	}
}

func newQQBotServer() *qqBotServer {
	logger := newLogger()

	// 编译正则
	reCQ := regexp.MustCompile(`\[CQ:[^\]]+\]`)
	// image/mface 都可能携带图片；mface 在部分实现中可能单独作为 CQ 段类型上报。
	reCQImage := regexp.MustCompile(`\[CQ:(?:image|mface),([^\]]+)\]`)
	reCQReply := regexp.MustCompile(`\[CQ:reply,([^\]]+)\]`)
	reBV := regexp.MustCompile(`BV[a-zA-Z0-9]{10}`)
	reBilibiliURL := regexp.MustCompile(`(https?://)?((www|m)\.)?bilibili\.com/video/(BV[a-zA-Z0-9]{10})`)
	reBilibiliShort := regexp.MustCompile(`https?://b23\.tv/[0-9A-Za-z]+`)
	reYouTube := []*regexp.Regexp{
		regexp.MustCompile(`(?:https?://)?(?:www\.)?youtube\.com/watch\?v=([a-zA-Z0-9_-]{11})`),
		regexp.MustCompile(`(?:https?://)?(?:www\.)?youtu\.be/([a-zA-Z0-9_-]{11})`),
		regexp.MustCompile(`watch\?v=([a-zA-Z0-9_-]{11})`),
	}
	reHost := regexp.MustCompile(`(?:https?://)?(?P<host>[^/:\s\?#]+)`)

	allowedUserSet := make(map[string]struct{}, len(allowedQQUsers))
	for _, id := range allowedQQUsers {
		allowedUserSet[id] = struct{}{}
	}
	allowedGroupSet := make(map[string]struct{}, len(allowedGroupIDs))
	for _, id := range allowedGroupIDs {
		allowedGroupSet[id] = struct{}{}
	}

	bot := &qqBotServer{
		shutdown:         make(chan struct{}),
		logger:           logger,
		users:            make(map[string]*userProfile),
		groupSettings:    groupSettings{BvEnabled: make(map[string]bool), ModelDefault: make(map[string]string)},
		modelPrompts:     make(map[string]string),
		defaultModel:     "gpt",
		nginxServers:     make(map[string]string),
		nginxServerConfs: make(map[string]string),
		nginxACL:         make(map[string][]string),
		steamWatch:       make(map[string]*steamWatchEntry),
		clients:          make(map[*wsClient]struct{}),
		httpClient:       newHTTPClient(),
		proxyClient:      newProxyHTTPClient(),
		reCQ:             reCQ,
		reCQImage:        reCQImage,
		reCQReply:        reCQReply,
		reBV:             reBV,
		reBilibiliURL:    reBilibiliURL,
		reBilibiliShort:  reBilibiliShort,
		reYouTube:        reYouTube,
		reHostExtract:    reHost,
		allowedUsers:     allowedUserSet,
		allowedGroups:    allowedGroupSet,
	}

	bot.loadMemory()
	bot.loadModelPrompts()
	bot.loadNginxServers()
	bot.loadSteamWatch()
	bot.startBackgroundSave()
	bot.startSteamMonitor()

	return bot
}

// ============ 内存管理 ============

func (b *qqBotServer) loadMemory() {
	b.memoryMu.Lock()
	defer b.memoryMu.Unlock()

	file, err := os.Open(memoryFile)
	if err != nil {
		// 文件不存在视为初始状态
		b.ensureGroupSettingsLocked()
		return
	}
	defer file.Close()

	var raw map[string]json.RawMessage
	if err := json.NewDecoder(file).Decode(&raw); err != nil {
		b.logger.Printf("加载记忆失败: %v", err)
		b.ensureGroupSettingsLocked()
		return
	}

	for k, v := range raw {
		if k == groupSettingsKey {
			var gs groupSettings
			if err := json.Unmarshal(v, &gs); err != nil {
				b.logger.Printf("解析 group_settings 失败: %v", err)
				continue
			}
			if gs.BvEnabled == nil {
				gs.BvEnabled = make(map[string]bool)
			}
			if gs.ModelDefault == nil {
				gs.ModelDefault = make(map[string]string)
			}
			b.groupSettings = gs
		} else {
			var up userProfile
			if err := json.Unmarshal(v, &up); err != nil {
				b.logger.Printf("解析 user profile 失败 (%s): %v", k, err)
				continue
			}
			b.users[k] = &up
		}
	}
	b.ensureGroupSettingsLocked()
}

func (b *qqBotServer) ensureGroupSettingsLocked() {
	if b.groupSettings.BvEnabled == nil {
		b.groupSettings.BvEnabled = make(map[string]bool)
	}
	if b.groupSettings.ModelDefault == nil {
		b.groupSettings.ModelDefault = make(map[string]string)
	}
}

func (b *qqBotServer) saveMemory() {
	b.memoryMu.RLock()
	defer b.memoryMu.RUnlock()

	data := make(map[string]any, len(b.users)+1)
	data[groupSettingsKey] = b.groupSettings
	for k, v := range b.users {
		data[k] = v
	}
	tmp := memoryFile + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		b.logger.Printf("保存记忆失败（创建临时文件）: %v", err)
		return
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	if err := enc.Encode(data); err != nil {
		b.logger.Printf("保存记忆失败（写入）: %v", err)
		_ = f.Close()
		_ = os.Remove(tmp)
		return
	}
	_ = f.Close()
	if err := os.Rename(tmp, memoryFile); err != nil {
		b.logger.Printf("保存记忆失败（重命名）: %v", err)
		return
	}
	b.memoryDirty = false
}

func (b *qqBotServer) startBackgroundSave() {
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				b.memoryMu.RLock()
				dirty := b.memoryDirty
				b.memoryMu.RUnlock()
				if dirty {
					b.saveMemory()
				}
			case <-b.shutdown:
				return
			}
		}
	}()
}

func (b *qqBotServer) ensureUser(qq string) *userProfile {
	up, ok := b.users[qq]
	if !ok {
		up = &userProfile{History: make([]historyItem, 0)}
		b.users[qq] = up
	}
	return up
}

func (b *qqBotServer) appendHistory(qq, role, content string) {
	b.memoryMu.Lock()
	defer b.memoryMu.Unlock()
	up := b.ensureUser(qq)
	up.History = append(up.History, historyItem{Role: role, Content: content})
	maxItems := 10 * 2 // MAX_HISTORY_TURNS * 2
	if len(up.History) > maxItems {
		up.History = up.History[len(up.History)-maxItems:]
	}
	b.memoryDirty = true
}

func (b *qqBotServer) appendUserEvent(qq, groupID, typ, detail string) {
	if qq == "" || qq == "0" || strings.TrimSpace(detail) == "" {
		return
	}
	b.memoryMu.Lock()
	defer b.memoryMu.Unlock()
	up := b.ensureUser(qq)
	up.Events = append(up.Events, userEvent{
		Time:    time.Now().Format(time.RFC3339),
		Type:    typ,
		GroupID: groupID,
		Detail:  detail,
	})
	if len(up.Events) > 80 {
		up.Events = up.Events[len(up.Events)-80:]
	}
	b.memoryDirty = true
}

func (b *qqBotServer) rememberBotAction(qq, groupID, command, result string) {
	command = strings.TrimSpace(command)
	result = strings.TrimSpace(result)
	if command == "" {
		return
	}
	if len(result) > 500 {
		result = result[:500] + "...(已截断)"
	}
	b.appendUserEvent(qq, groupID, "bot_command", fmt.Sprintf("执行命令: %s；结果: %s", command, result))
}

func (b *qqBotServer) recentUserEvents(qq string, limit int) []userEvent {
	b.memoryMu.RLock()
	defer b.memoryMu.RUnlock()
	up, ok := b.users[qq]
	if !ok || len(up.Events) == 0 {
		return nil
	}
	start := len(up.Events) - limit
	if start < 0 {
		start = 0
	}
	out := make([]userEvent, 0, len(up.Events)-start)
	out = append(out, up.Events[start:]...)
	return out
}

func (b *qqBotServer) clearUserMemory(qq string) bool {
	b.memoryMu.Lock()
	defer b.memoryMu.Unlock()
	if up, ok := b.users[qq]; ok {
		up.History = nil
		b.memoryDirty = true
		return true
	}
	return false
}

// 模型与群设置

func (b *qqBotServer) getUserModel(qq string, groupID *string) string {
	b.memoryMu.RLock()
	defer b.memoryMu.RUnlock()
	if up, ok := b.users[qq]; ok && up.Model != "" {
		return up.Model
	}
	if groupID != nil {
		if m, ok := b.groupSettings.ModelDefault[*groupID]; ok && m != "" {
			return m
		}
	}
	return b.defaultModel
}

func (b *qqBotServer) setUserModel(qq, model string) bool {
	if model != "claude" && model != "gpt" && model != "grok" && model != "deepseek" {
		return false
	}
	b.memoryMu.Lock()
	defer b.memoryMu.Unlock()
	up := b.ensureUser(qq)
	up.Model = model
	b.memoryDirty = true
	return true
}

func (b *qqBotServer) setGroupDefaultModel(groupID, model string) bool {
	if model != "gpt" && model != "claude" && model != "grok" && model != "deepseek" {
		return false
	}
	b.memoryMu.Lock()
	defer b.memoryMu.Unlock()
	if b.groupSettings.ModelDefault == nil {
		b.groupSettings.ModelDefault = make(map[string]string)
	}
	b.groupSettings.ModelDefault[groupID] = model
	b.memoryDirty = true
	return true
}

func (b *qqBotServer) isGroupBVEnabled(groupID string) bool {
	b.memoryMu.RLock()
	defer b.memoryMu.RUnlock()
	if groupID == "" {
		return true
	}
	if b.groupSettings.BvEnabled == nil {
		return true
	}
	enabled, ok := b.groupSettings.BvEnabled[groupID]
	if !ok {
		return true
	}
	return enabled
}

func (b *qqBotServer) setGroupBVEnabled(groupID string, enabled bool) {
	b.memoryMu.Lock()
	defer b.memoryMu.Unlock()
	if b.groupSettings.BvEnabled == nil {
		b.groupSettings.BvEnabled = make(map[string]bool)
	}
	b.groupSettings.BvEnabled[groupID] = enabled
	b.memoryDirty = true
}

// ============ 提示词管理 ============

func (b *qqBotServer) loadModelPrompts() {
	defaults := map[string]string{
		"grok":     "你是Grok，语气轻松幽默。",
		"gpt":      "你是AI助手，请简洁回答。",
		"claude":   "你是Claude，请详细且逻辑清晰地回答。",
		"deepseek": "你是DeepSeek，请用中文给出严谨、直接、可执行的回答。",
	}
	for model, filename := range promptFiles {
		content, err := os.ReadFile(filename)
		if err != nil {
			b.modelPrompts[model] = defaults[model]
			continue
		}
		text := strings.TrimSpace(string(content))
		if text == "" {
			text = defaults[model]
		}
		b.modelPrompts[model] = text
		b.logger.Printf("已加载提示词文件: %s", filename)
	}
}

// ============ Nginx 服务器配置 ============

func (b *qqBotServer) loadNginxServers() {
	b.nginxMu.Lock()
	defer b.nginxMu.Unlock()

	file, err := os.Open(nginxServersFile)
	if err != nil {
		return
	}
	defer file.Close()

	var raw map[string]json.RawMessage
	if err := json.NewDecoder(file).Decode(&raw); err != nil {
		b.logger.Printf("加载被控服务器配置失败: %v", err)
		return
	}

	var cfg nginxConfigFile
	cfg.Conf = make(map[string]string)
	cfg.ACL = make(map[string][]string)
	cfg.Servers = make(map[string]string)

	for k, v := range raw {
		if k == "__default__" {
			_ = json.Unmarshal(v, &cfg.Default)
		} else if k == "__conf__" {
			_ = json.Unmarshal(v, &cfg.Conf)
		} else if k == "__acl__" {
			_ = json.Unmarshal(v, &cfg.ACL)
		} else {
			var addr string
			if err := json.Unmarshal(v, &addr); err == nil {
				cfg.Servers[k] = addr
			}
		}
	}

	b.nginxDefault = cfg.Default
	for k, v := range cfg.Conf {
		b.nginxServerConfs[k] = v
	}
	for k, v := range cfg.ACL {
		b.nginxACL[k] = v
	}
	for k, v := range cfg.Servers {
		b.nginxServers[k] = v
	}
}

func (b *qqBotServer) saveNginxServersLocked() {
	data := make(map[string]any)
	if b.nginxDefault != "" {
		data["__default__"] = b.nginxDefault
	}
	if len(b.nginxServerConfs) > 0 {
		data["__conf__"] = b.nginxServerConfs
	}
	if len(b.nginxACL) > 0 {
		data["__acl__"] = b.nginxACL
	}
	for k, v := range b.nginxServers {
		data[k] = v
	}
	tmp := nginxServersFile + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		b.logger.Printf("保存被控服务器配置失败（创建临时文件）: %v", err)
		return
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	if err := enc.Encode(data); err != nil {
		b.logger.Printf("保存被控服务器配置失败: %v", err)
		_ = f.Close()
		_ = os.Remove(tmp)
		return
	}
	_ = f.Close()
	if err := os.Rename(tmp, nginxServersFile); err != nil {
		b.logger.Printf("保存被控服务器配置失败（重命名）: %v", err)
	}
}

func (b *qqBotServer) isGlobalAdmin(userID string, msgType string) bool {
	if msgType != "private" {
		return false
	}
	_, ok := b.allowedUsers[userID]
	return ok
}

// 返回 (name, host, port, ok)
func (b *qqBotServer) getDefaultNginxServer() (string, string, int, bool) {
	b.nginxMu.RLock()
	defer b.nginxMu.RUnlock()
	if len(b.nginxServers) == 0 {
		return "", "", 0, false
	}
	name := b.nginxDefault
	addr := ""
	if name != "" {
		if v, ok := b.nginxServers[name]; ok {
			addr = v
		}
	}
	if addr == "" {
		for k, v := range b.nginxServers {
			name = k
			addr = v
			break
		}
	}
	if addr == "" {
		return "", "", 0, false
	}
	host, portStr, ok := strings.Cut(addr, ":")
	if !ok || host == "" {
		return "", "", 0, false
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 || port > 65535 {
		return "", "", 0, false
	}
	return name, strings.TrimSpace(host), port, true
}

// ============ Nginx 本地操作 ============

func (b *qqBotServer) runShellCommand(ctx context.Context, name string, args ...string) (int, string) {
	cmd := exec.CommandContext(ctx, name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return exitErr.ExitCode(), strings.TrimSpace(string(out))
		}
		return 1, fmt.Sprintf("执行命令异常: %v, 输出: %s", err, strings.TrimSpace(string(out)))
	}
	return 0, strings.TrimSpace(string(out))
}

func (b *qqBotServer) reloadNginx(ctx context.Context) (bool, string) {
	code, output := b.runShellCommand(ctx, "nginx", "-t")
	if code != 0 {
		return false, "❌ nginx 配置测试失败 (nginx -t)\n" + output
	}
	type tryCmd struct {
		Name string
		Args []string
	}
	cmds := []tryCmd{
		{"nginx", []string{"-s", "reload"}},
		{"systemctl", []string{"reload", "nginx"}},
		{"systemctl", []string{"restart", "nginx"}},
	}
	var tried []string
	for _, c := range cmds {
		code, out := b.runShellCommand(ctx, c.Name, c.Args...)
		tried = append(tried, fmt.Sprintf("- %s %s (exit %d)\n  输出: %s", c.Name, strings.Join(c.Args, " "), code, out))
		if code == 0 {
			return true, fmt.Sprintf("✅ 已执行: %s %s\n%s", c.Name, strings.Join(c.Args, " "), out)
		}
	}
	return false, "❌ 所有 Nginx 重载命令均执行失败，已保留配置文件，请手动检查：\n" + strings.Join(tried, "\n")
}

func (b *qqBotServer) nginxTestConfig(ctx context.Context) string {
	code, output := b.runShellCommand(ctx, "nginx", "-t")
	if code == 0 {
		return "✅ nginx 配置语法检测通过 (nginx -t)\n" + output
	}
	return "❌ nginx 配置语法检测失败 (nginx -t)\n" + output
}

// 解析 stream 配置文件，输出概要。
func parseNginxStreamSummary(content, filename string) []string {
	results := []string{}
	metaRe := regexp.MustCompile(`^#\s*name=(?P<name>\S+)\s+target=(?P<host>[^:\s]+):(?P<tport>\d+)\s+listen=(?P<lport>\d+)`)
	for _, m := range metaRe.FindAllStringSubmatch(content, -1) {
		if len(m) < 5 {
			continue
		}
		name := m[1]
		host := m[2]
		tport := m[3]
		lport := m[4]
		results = append(results, fmt.Sprintf("%s: %s:%s -> 0.0.0.0:%s", name, host, tport, lport))
	}
	// upstream + server 推导
	upMap := map[string][2]string{}
	upRe := regexp.MustCompile(`upstream\s+([a-zA-Z0-9_-]+)\s*{([^}]*)}`)
	serverRe := regexp.MustCompile(`server\s+([0-9a-zA-Z\.\-]+):(\d+);`)
	for _, m := range upRe.FindAllStringSubmatch(content, -1) {
		if len(m) < 3 {
			continue
		}
		upName := m[1]
		body := m[2]
		if s := serverRe.FindStringSubmatch(body); len(s) >= 3 {
			upMap[upName] = [2]string{s[1], s[2]}
		}
	}
	serverBlockRe := regexp.MustCompile(`server\s*{([^}]*)}`)
	proxyPassRe := regexp.MustCompile(`proxy_pass\s+([a-zA-Z0-9_-]+);`)
	listenRe := regexp.MustCompile(`listen\s+(?:[0-9\.\:]+\:)?(\d+)\b`)
	seen := map[string]struct{}{}
	for _, m := range serverBlockRe.FindAllStringSubmatch(content, -1) {
		if len(m) < 2 {
			continue
		}
		body := m[1]
		p := proxyPassRe.FindStringSubmatch(body)
		if len(p) < 2 {
			continue
		}
		upName := p[1]
		up, ok := upMap[upName]
		if !ok {
			continue
		}
		lp := listenRe.FindStringSubmatch(body)
		if len(lp) < 2 {
			continue
		}
		lport := lp[1]
		host := up[0]
		tport := up[1]
		key := fmt.Sprintf("%s|%s|%s|%s", upName, host, tport, lport)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		results = append(results, fmt.Sprintf("%s: %s:%s -> 0.0.0.0:%s", upName, host, tport, lport))
	}
	if len(results) == 0 {
		return []string{fmt.Sprintf("%s (自定义配置，未解析详细信息)", filename)}
	}
	return results
}

func (b *qqBotServer) nginxListConfigs() string {
	if fi, err := os.Stat(nginxStreamConfDir); err != nil || !fi.IsDir() {
		return fmt.Sprintf("❌ 目录不存在: %s\n请确认已在 nginx.conf 中正确配置 include。", nginxStreamConfDir)
	}
	entries, err := os.ReadDir(nginxStreamConfDir)
	if err != nil {
		return fmt.Sprintf("❌ 读取目录失败: %v", err)
	}
	var files []string
	for _, e := range entries {
		if !e.Type().IsRegular() {
			continue
		}
		if strings.HasSuffix(e.Name(), ".conf") {
			files = append(files, e.Name())
		}
	}
	if len(files) == 0 {
		return "📂 当前没有任何 stream 配置 (.conf)。"
	}
	lines := []string{"📂 Nginx stream 配置列表:"}
	for _, fn := range files {
		path := filepath.Join(nginxStreamConfDir, fn)
		content, err := os.ReadFile(path)
		if err != nil {
			lines = append(lines, fmt.Sprintf("- %s: 读取失败: %v", fn, err))
			continue
		}
		for _, s := range parseNginxStreamSummary(string(content), fn) {
			lines = append(lines, fmt.Sprintf("- %s  [%s]", s, fn))
		}
	}
	return strings.Join(lines, "\n")
}

func (b *qqBotServer) nginxAddConfig(ctx context.Context, name, targetHost, targetPort, listenPort string) string {
	if !regexp.MustCompile(`^[a-zA-Z0-9_-]+$`).MatchString(name) {
		return "❌ 名字只允许使用字母、数字、下划线、短横线。"
	}
	if strings.Contains(targetHost, " ") || strings.Contains(targetHost, "/") {
		return "❌ 转发地址格式不合法，只允许域名或 IP，不要包含协议/http://。"
	}
	tPort, err1 := strconv.Atoi(targetPort)
	lPort, err2 := strconv.Atoi(listenPort)
	if err1 != nil || err2 != nil {
		return "❌ 端口号必须是数字。"
	}
	if tPort < 1 || tPort > 65535 || lPort < 1 || lPort > 65535 {
		return "❌ 端口号必须在 1-65535 之间。"
	}
	if err := os.MkdirAll(nginxStreamConfDir, 0o755); err != nil {
		return fmt.Sprintf("❌ 创建目录失败: %v", err)
	}
	confPath := filepath.Join(nginxStreamConfDir, name+".conf")
	backupMsg := ""
	if _, err := os.Stat(confPath); err == nil {
		ts := time.Now().Format("20060102150405")
		backupPath := confPath + ".bak." + ts
		if err := copyFile(confPath, backupPath); err != nil {
			backupMsg = fmt.Sprintf("\n⚠️ 旧配置备份失败: %v", err)
		} else {
			backupMsg = "\nℹ️ 已备份旧配置为: " + backupPath
		}
	}
	content := fmt.Sprintf(`# name=%s target=%s:%d listen=%d  auto=qqbot
stream {
    resolver 1.1.1.1 valid=300s ipv6=on;

    upstream %s {
        server %s:%d;
    }

    # 支持 TCP 协议
    server {
        listen 0.0.0.0:%d;
        proxy_connect_timeout 5s;
        proxy_timeout 600s;
        proxy_pass %s;
    }

    # 支持 UDP 协议
    server {
        listen 0.0.0.0:%d udp;
        proxy_connect_timeout 5s;
        proxy_timeout 600s;
        proxy_pass %s;
    }
}
`, name, targetHost, tPort, lPort, name, targetHost, tPort, lPort, name, lPort, name)
	if err := os.WriteFile(confPath, []byte(content), 0o644); err != nil {
		if os.IsPermission(err) {
			return fmt.Sprintf("❌ 写入失败，没有权限写入 %s，请确保机器人进程具有 root 权限。", confPath)
		}
		return fmt.Sprintf("❌ 写入配置失败: %v", err)
	}
	ok, reloadMsg := b.reloadNginx(ctx)
	prefix := fmt.Sprintf("✅ 已写入配置: %s%s\n", confPath, backupMsg)
	if ok {
		return prefix + reloadMsg
	}
	return prefix + reloadMsg
}

func (b *qqBotServer) nginxRemoveConfig(ctx context.Context, name string) string {
	confPath := filepath.Join(nginxStreamConfDir, name+".conf")
	if _, err := os.Stat(confPath); err != nil {
		if os.IsNotExist(err) {
			return "❌ 未找到配置文件: " + confPath
		}
		return fmt.Sprintf("❌ 访问配置文件失败: %v", err)
	}
	ts := time.Now().Format("20060102150405")
	backupPath := confPath + ".bak." + ts
	backupMsg := ""
	if err := copyFile(confPath, backupPath); err != nil {
		backupMsg = fmt.Sprintf("\n⚠️ 删除前备份失败: %v", err)
	} else {
		backupMsg = "\nℹ️ 已备份为: " + backupPath
	}
	if err := os.Remove(confPath); err != nil {
		if os.IsPermission(err) {
			return fmt.Sprintf("❌ 删除失败，没有权限删除 %s，请确保机器人进程具有 root 权限。", confPath)
		}
		return fmt.Sprintf("❌ 删除配置失败: %v", err)
	}
	ok, reloadMsg := b.reloadNginx(ctx)
	prefix := fmt.Sprintf("✅ 已删除配置: %s%s\n", confPath, backupMsg)
	if ok {
		return prefix + reloadMsg
	}
	return prefix + reloadMsg
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}

// ============ 远程 NginxAgent 调用 ============

func (b *qqBotServer) callRemoteNginx(ctx context.Context, serverName, host string, port int, cmd string, params map[string]any) (bool, string) {
	u := fmt.Sprintf("ws://%s:%d", host, port)
	dialer := websocket.Dialer{
		HandshakeTimeout: 5 * time.Second,
	}
	conn, _, err := dialer.DialContext(ctx, u, nil)
	if err != nil {
		return false, fmt.Sprintf("⚠️ 无法连接到被控服务器 %s (%s): %v", serverName, u, err)
	}
	defer conn.Close()

	payload := map[string]any{
		"type":   "nginx_cmd",
		"cmd":    cmd,
		"params": params,
	}
	if err := conn.WriteJSON(payload); err != nil {
		return false, fmt.Sprintf("⚠️ 已连接 %s，但发送 %s 指令失败: %v", u, cmd, err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		return false, fmt.Sprintf("⚠️ 已连接 %s，但接收 %s 返回失败: %v", u, cmd, err)
	}
	var resp struct {
		Type    string `json:"type"`
		OK      bool   `json:"ok"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(msg, &resp); err != nil {
		return false, fmt.Sprintf("⚠️ 来自 %s (%s) 的返回不是合法 JSON。", serverName, u)
	}
	if resp.Type != "nginx_result" {
		return false, fmt.Sprintf("⚠️ 来自 %s (%s) 的返回类型异常: %s", serverName, u, resp.Type)
	}
	prefix := fmt.Sprintf("🎯 目标服务器: %s (%s)\n", serverName, u)
	return resp.OK, prefix + resp.Message
}

func (b *qqBotServer) testRemoteNginxServer(ctx context.Context, name, host string, port int) (bool, string) {
	return b.callRemoteNginx(ctx, name, host, port, "test", map[string]any{})
}

// ============ Nginx 指令处理 ============

func (b *qqBotServer) handleNginxServerCommand(parts []string, userID, msgType string) string {
	if !b.isGlobalAdmin(userID, msgType) {
		return "❌ 只有白名单私聊管理员可以管理被控服务器。"
	}
	if len(parts) == 0 {
		return "🌐 Nginx 被控服务器管理:\n" +
			"/nginx server list                 查看所有被控服务器\n" +
			"/nginx server add 名字 地址:端口   新增/更新被控服务器\n" +
			"  例如: /nginx server add s1 1.2.3.4:9876\n" +
			"/nginx server rm 名字              删除被控服务器"
	}
	action := strings.ToLower(parts[0])

	b.nginxMu.Lock()
	defer b.nginxMu.Unlock()

	switch action {
	case "list":
		if len(b.nginxServers) == 0 {
			return "📂 当前没有配置任何被控服务器。"
		}
		lines := []string{"📂 被控服务器列表:"}
		for name, addr := range b.nginxServers {
			lines = append(lines, fmt.Sprintf("- %s: %s", name, addr))
		}
		return strings.Join(lines, "\n")
	case "add":
		if len(parts) != 3 {
			return "❌ 用法错误: /nginx server add [名字] [地址]:[端口]\n" +
				"示例: /nginx server add s1 1.2.3.4:9876"
		}
		name := parts[1]
		addr := parts[2]
		if !regexp.MustCompile(`^[a-zA-Z0-9_-]+$`).MatchString(name) {
			return "❌ 名字只允许使用字母、数字、下划线、短横线。"
		}
		host, portStr, ok := strings.Cut(addr, ":")
		if !ok {
			return "❌ 地址格式错误，应为 [主机]:[端口]，例如 1.2.3.4:9876 或 agent.example.com:9876"
		}
		host = strings.TrimSpace(host)
		if host == "" {
			return "❌ 主机名不能为空。"
		}
		port, err := strconv.Atoi(portStr)
		if err != nil || port < 1 || port > 65535 {
			return "❌ 端口必须在 1-65535 之间。"
		}
		value := fmt.Sprintf("%s:%d", host, port)
		b.nginxServers[name] = value
		if b.nginxDefault == "" {
			b.nginxDefault = name
		}
		b.saveNginxServersLocked()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		ok2, testMsg := b.testRemoteNginxServer(ctx, name, host, port)
		_ = ok2
		return fmt.Sprintf("✅ 已添加/更新被控服务器: %s -> %s\n%s", name, value, testMsg)
	case "rm":
		if len(parts) != 2 {
			return "❌ 用法错误: /nginx server rm [名字]"
		}
		name := parts[1]
		if _, ok := b.nginxServers[name]; !ok {
			return fmt.Sprintf("❌ 未找到被控服务器: %s", name)
		}
		delete(b.nginxServers, name)
		if b.nginxDefault == name {
			b.nginxDefault = ""
		}
		delete(b.nginxServerConfs, name)
		delete(b.nginxACL, name)
		b.saveNginxServersLocked()
		return fmt.Sprintf("✅ 已删除被控服务器: %s", name)
	default:
		return "❌ 未知 server 子命令: " + action + "\n" +
			"可用子命令: list / add / rm\n" +
			"示例: /nginx server add s1 1.2.3.4:9876"
	}
}

func (b *qqBotServer) handleNginxQQCommand(parts []string, userID, msgType string) string {
	if !b.isGlobalAdmin(userID, msgType) {
		return "❌ 只有白名单私聊管理员可以管理 Nginx QQ 权限。"
	}
	if len(parts) == 0 {
		return "🔐 Nginx QQ 权限管理:\n" +
			"/nginx qq add 服务器名 QQ   添加某 QQ 为该服务器编辑者\n" +
			"/nginx qq rm 服务器名 QQ    删除某 QQ 的编辑权限\n" +
			"/nginx qq list [服务器名]   查看某服务器的授权列表"
	}
	action := strings.ToLower(parts[0])

	b.nginxMu.Lock()
	defer b.nginxMu.Unlock()

	switch action {
	case "add", "addr":
		if len(parts) != 3 {
			return "❌ 用法错误: /nginx qq add [服务器名] [QQ号]"
		}
		sName := parts[1]
		qq := parts[2]
		if _, ok := b.nginxServers[sName]; !ok {
			return fmt.Sprintf("❌ 未找到被控服务器: %s", sName)
		}
		acl := b.nginxACL[sName]
		for _, id := range acl {
			if id == qq {
				return fmt.Sprintf("✅ 已为服务器 %s 授权 QQ: %s\n当前授权用户: %s", sName, qq, strings.Join(acl, ", "))
			}
		}
		acl = append(acl, qq)
		b.nginxACL[sName] = acl
		b.saveNginxServersLocked()
		return fmt.Sprintf("✅ 已为服务器 %s 授权 QQ: %s\n当前授权用户: %s", sName, qq, strings.Join(acl, ", "))
	case "rm":
		if len(parts) != 3 {
			return "❌ 用法错误: /nginx qq rm [服务器名] [QQ号]"
		}
		sName := parts[1]
		qq := parts[2]
		if _, ok := b.nginxServers[sName]; !ok {
			return fmt.Sprintf("❌ 未找到被控服务器: %s", sName)
		}
		acl := b.nginxACL[sName]
		newACL := make([]string, 0, len(acl))
		found := false
		for _, id := range acl {
			if id == qq {
				found = true
				continue
			}
			newACL = append(newACL, id)
		}
		if !found {
			return fmt.Sprintf("ℹ️ QQ %s 本来就没有 %s 的编辑权限。", qq, sName)
		}
		if len(newACL) == 0 {
			delete(b.nginxACL, sName)
		} else {
			b.nginxACL[sName] = newACL
		}
		b.saveNginxServersLocked()
		return fmt.Sprintf("✅ 已从服务器 %s 移除 QQ 授权: %s", sName, qq)
	case "list":
		if len(parts) == 1 {
			if len(b.nginxACL) == 0 {
				return "📂 当前没有为任何服务器配置 QQ 权限。"
			}
			lines := []string{"📂 Nginx QQ 权限列表:"}
			for sName, acl := range b.nginxACL {
				val := "无"
				if len(acl) > 0 {
					val = strings.Join(acl, ", ")
				}
				lines = append(lines, fmt.Sprintf("- %s: %s", sName, val))
			}
			return strings.Join(lines, "\n")
		}
		if len(parts) == 2 {
			sName := parts[1]
			if _, ok := b.nginxServers[sName]; !ok {
				return fmt.Sprintf("❌ 未找到被控服务器: %s", sName)
			}
			acl := b.nginxACL[sName]
			val := "无"
			if len(acl) > 0 {
				val = strings.Join(acl, ", ")
			}
			return fmt.Sprintf("📂 服务器 %s 授权 QQ 列表:\n%s", sName, val)
		}
		return "❌ 用法错误: /nginx qq list [服务器名]"
	default:
		return "❌ 未知 qq 子命令: " + action + "\n可用子命令: add / rm / list"
	}
}

func (b *qqBotServer) handleNginxCommand(rawArgs, userID, msgType string) string {
	args := strings.TrimSpace(rawArgs)
	name, host, port, hasDefault := b.getDefaultNginxServer()
	isAdmin := b.isGlobalAdmin(userID, msgType)

	if args == "" {
		return "🌐 Nginx 管理用法:\n" +
			"/nginx list                查看当前所有 stream 配置\n" +
			"/nginx add 名字 地址 远端端口 本地端口\n" +
			"  例如: /nginx add jpp jpp.yamatu.xyz 46569 10197\n" +
			"/nginx rm 名字            删除指定名字的配置\n" +
			"/nginx -t                  仅测试 nginx 配置语法 (nginx -t)\n" +
			"/nginx mkdir 文件名        在远程创建/选择配置文件\n" +
			"/nginx set 服务器名|local  切换默认被控服务器\n" +
			"/nginx server ...          管理被控服务器 (list/add/rm)"
	}
	parts := strings.Fields(args)
	sub := strings.ToLower(parts[0])

	if sub == "-h" || sub == "--help" || sub == "help" {
		return "🌐 Nginx 管理用法:\n" +
			"/nginx list                查看当前所有 stream 配置\n" +
			"/nginx add 名字 地址 远端端口 本地端口\n" +
			"  示例: /nginx add jpp jpp.yamatu.xyz 46569 10197\n" +
			"/nginx rm 名字            删除指定名字的配置\n" +
			"/nginx -t                  测试 nginx 配置语法 (nginx -t)\n" +
			"/nginx mkdir 文件名        在被控服务器上创建/选择配置文件\n" +
			"  示例: /nginx mkdir forword\n" +
			"/nginx set 服务器名|local  切换默认被控服务器\n" +
			"  示例: /nginx set jpix\n" +
			"/nginx qq add 服务器名 QQ  授权 QQ 编辑指定服务器配置\n" +
			"/nginx qq rm 服务器名 QQ   撤销 QQ 的编辑权限\n" +
			"/nginx server ...          管理被控服务器 (list/add/rm)\n" +
			"  示例: /nginx server add jpix 1.2.3.4:10190"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	switch sub {
	case "list":
		if hasDefault {
			ok, msg := b.callRemoteNginx(ctx, name, host, port, "list", map[string]any{})
			_ = ok
			return msg
		}
		return b.nginxListConfigs()
	case "-t", "test", "check":
		if hasDefault {
			ok, msg := b.callRemoteNginx(ctx, name, host, port, "test", map[string]any{})
			_ = ok
			return msg
		}
		return b.nginxTestConfig(ctx)
	case "mkdir":
		if !hasDefault {
			return "❌ 当前未配置任何被控服务器，请先使用 /nginx server add 添加。"
		}
		if len(parts) != 2 {
			return "❌ 用法错误: /nginx mkdir [文件名]"
		}
		confName := parts[1]
		// 权限检查
		b.nginxMu.RLock()
		if !isAdmin {
			acl := b.nginxACL[name]
			allowed := false
			for _, id := range acl {
				if id == userID {
					allowed = true
					break
				}
			}
			if !allowed {
				b.nginxMu.RUnlock()
				return fmt.Sprintf("❌ 你没有权限修改服务器 %s 的配置，请联系管理员使用 /nginx qq add 授权。", name)
			}
		}
		b.nginxMu.RUnlock()

		b.nginxMu.Lock()
		b.nginxServerConfs[name] = confName
		b.saveNginxServersLocked()
		b.nginxMu.Unlock()

		ok, msg := b.callRemoteNginx(ctx, name, host, port, "mkdir", map[string]any{"conf": confName})
		_ = ok
		return msg
	case "set":
		if !isAdmin {
			return "❌ 只有白名单私聊管理员可以使用 /nginx set 切换默认服务器。"
		}
		if len(parts) != 2 {
			return "❌ 用法错误: /nginx set [服务器名|local]"
		}
		target := parts[1]
		b.nginxMu.Lock()
		defer b.nginxMu.Unlock()
		if strings.EqualFold(target, "local") {
			b.nginxDefault = ""
			b.saveNginxServersLocked()
			return "✅ 已切换到本机 Nginx，不再使用远程被控服务器。"
		}
		if _, ok := b.nginxServers[target]; !ok {
			return fmt.Sprintf("❌ 未找到被控服务器: %s", target)
		}
		b.nginxDefault = target
		b.saveNginxServersLocked()
		addr := b.nginxServers[target]
		confName := b.nginxServerConfs[target]
		extra := "，尚未选择配置文件，请先 /nginx mkdir [文件名]"
		if confName != "" {
			extra = "，当前配置文件: " + confName + ".conf"
		}
		return fmt.Sprintf("✅ 默认被控服务器已切换为: %s -> %s%s", target, addr, extra)
	case "server":
		return b.handleNginxServerCommand(parts[1:], userID, msgType)
	case "qq":
		return b.handleNginxQQCommand(parts[1:], userID, msgType)
	case "add":
		if len(parts) != 5 {
			return "❌ 用法错误: /nginx add [名字] [转发地址] [目标端口] [本地端口]\n" +
				"示例: /nginx add jpp jpp.yamatu.xyz 46569 10197"
		}
		nameArg := parts[1]
		hostArg := parts[2]
		tPort := parts[3]
		lPort := parts[4]
		if hasDefault {
			// 权限检查
			b.nginxMu.RLock()
			if !isAdmin {
				acl := b.nginxACL[name]
				allowed := false
				for _, id := range acl {
					if id == userID {
						allowed = true
						break
					}
				}
				if !allowed {
					b.nginxMu.RUnlock()
					return fmt.Sprintf("❌ 你没有权限修改服务器 %s 的配置，请联系管理员使用 /nginx qq add 授权。", name)
				}
			}
			confName := b.nginxServerConfs[name]
			b.nginxMu.RUnlock()
			if confName == "" {
				return fmt.Sprintf("❌ 当前默认服务器 %s 尚未选择配置文件，请先使用 /nginx mkdir [文件名]", name)
			}
			params := map[string]any{
				"conf":        confName,
				"name":        nameArg,
				"target_host": hostArg,
				"target_port": tPort,
				"listen_port": lPort,
			}
			ok, msg := b.callRemoteNginx(ctx, name, host, port, "add", params)
			_ = ok
			return msg
		}
		return b.nginxAddConfig(ctx, nameArg, hostArg, tPort, lPort)
	case "rm":
		if len(parts) != 2 {
			return "❌ 用法错误: /nginx rm [名字]"
		}
		nameArg := parts[1]
		if hasDefault {
			b.nginxMu.RLock()
			if !isAdmin {
				acl := b.nginxACL[name]
				allowed := false
				for _, id := range acl {
					if id == userID {
						allowed = true
						break
					}
				}
				if !allowed {
					b.nginxMu.RUnlock()
					return fmt.Sprintf("❌ 你没有权限修改服务器 %s 的配置，请联系管理员使用 /nginx qq add 授权。", name)
				}
			}
			confName := b.nginxServerConfs[name]
			b.nginxMu.RUnlock()
			if confName == "" {
				return fmt.Sprintf("❌ 当前默认服务器 %s 尚未选择配置文件，请先使用 /nginx mkdir [文件名]", name)
			}
			params := map[string]any{
				"conf": confName,
				"name": nameArg,
			}
			ok, msg := b.callRemoteNginx(ctx, name, host, port, "rm", params)
			_ = ok
			return msg
		}
		return b.nginxRemoveConfig(ctx, nameArg)
	default:
		return fmt.Sprintf("❌ 未知子命令: %s\n可用子命令: list / add / rm / -t / server\n示例: /nginx add jpp jpp.yamatu.xyz 46569 10197\n      /nginx -t\n      /nginx server list", sub)
	}
}

// ============ 业务 API：Ping / 天气 / Epic / B站 / YouTube ============

func (b *qqBotServer) pingViaProxy(rawInput string) string {
	rawInput = strings.TrimSpace(rawInput)
	if rawInput == "" {
		return "❌ 请输入域名或IP"
	}
	m := b.reHostExtract.FindStringSubmatch(rawInput)
	if len(m) < 2 {
		return "❌ 无法解析域名: " + rawInput
	}
	host := m[1]
	targetURL := "https://" + host
	start := time.Now()
	client := b.proxyClient
	if client == nil {
		client = b.httpClient
	}
	req, err := http.NewRequest(http.MethodGet, targetURL, nil)
	if err != nil {
		return fmt.Sprintf("❌ Proxy Ping 失败\n🎯 目标: %s\n⚠️ 错误: %v", host, err)
	}
	req = req.WithContext(context.Background())
	resp, err := client.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return fmt.Sprintf("❌ Proxy Ping 超时\n🎯 目标: %s\n⏳ 超过10s无响应", host)
		}
		return fmt.Sprintf("❌ Proxy Ping 失败\n🎯 目标: %s\n⚠️ 错误: %v", host, err)
	}
	defer resp.Body.Close()
	latency := time.Since(start).Seconds() * 1000
	icon := "✅"
	if resp.StatusCode >= 400 {
		icon = "⚠️"
	}
	return fmt.Sprintf("%s Proxy Ping\n🎯 目标: %s\n📶 延迟: %.2fms\n🔢 状态码: %d", icon, host, latency, resp.StatusCode)
}

func (b *qqBotServer) getWeather(cityName string) string {
	if cityName == "" {
		return "❌ 请输入城市名"
	}
	if amapAPIKey == "" {
		return "❌ 未配置高德地图 API Key (AMAP_API_KEY)"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// 1. 地址转 adcode
	geoURL := "https://restapi.amap.com/v3/geocode/geo"
	req1, _ := http.NewRequestWithContext(ctx, http.MethodGet, geoURL, nil)
	q1 := req1.URL.Query()
	q1.Set("key", amapAPIKey)
	q1.Set("address", cityName)
	req1.URL.RawQuery = q1.Encode()
	resp1, err := b.httpClient.Do(req1)
	if err != nil {
		return fmt.Sprintf("❌ API错误: %v", err)
	}
	defer resp1.Body.Close()
	var geoResp struct {
		Geocodes []struct {
			Adcode string `json:"adcode"`
		} `json:"geocodes"`
	}
	if err := json.NewDecoder(resp1.Body).Decode(&geoResp); err != nil {
		return fmt.Sprintf("❌ API错误: %v", err)
	}
	if len(geoResp.Geocodes) == 0 {
		return "❌ 未找到该地区"
	}
	adcode := geoResp.Geocodes[0].Adcode

	// 2. 查询天气
	weatherURL := "https://restapi.amap.com/v3/weather/weatherInfo"
	req2, _ := http.NewRequestWithContext(ctx, http.MethodGet, weatherURL, nil)
	q2 := req2.URL.Query()
	q2.Set("key", amapAPIKey)
	q2.Set("city", adcode)
	q2.Set("extensions", "base")
	req2.URL.RawQuery = q2.Encode()
	resp2, err := b.httpClient.Do(req2)
	if err != nil {
		return fmt.Sprintf("❌ API错误: %v", err)
	}
	defer resp2.Body.Close()
	var wResp struct {
		Lives []struct {
			Province      string `json:"province"`
			City          string `json:"city"`
			Weather       string `json:"weather"`
			Temperature   string `json:"temperature"`
			WindDirection string `json:"winddirection"`
			WindPower     string `json:"windpower"`
			Humidity      string `json:"humidity"`
		} `json:"lives"`
	}
	if err := json.NewDecoder(resp2.Body).Decode(&wResp); err != nil {
		return fmt.Sprintf("❌ API错误: %v", err)
	}
	if len(wResp.Lives) == 0 {
		return "❌ 天气查询失败"
	}
	w := wResp.Lives[0]
	return fmt.Sprintf("🌤️ %s %s 天气\n%s %s℃\n%s风 %s级\n湿度: %s%%",
		w.Province, w.City, w.Weather, w.Temperature, w.WindDirection, w.WindPower, w.Humidity)
}

func (b *qqBotServer) getEpicFreeGames() string {
	url := "https://store-site-backend-static-ipv4.ak.epicgames.com/freeGamesPromotions?locale=zh-CN&country=CN&allowCountries=CN"
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	resp, err := b.httpClient.Do(req)
	if err != nil {
		return fmt.Sprintf("❌ Epic查询失败: %v", err)
	}
	defer resp.Body.Close()
	var raw struct {
		Data struct {
			Catalog struct {
				SearchStore struct {
					Elements []struct {
						Title       string `json:"title"`
						ProductSlug string `json:"productSlug"`
						URLSlug     string `json:"urlSlug"`
						Promotions  struct {
							PromotionalOffers []struct {
								PromotionalOffers []struct {
									DiscountSetting struct {
										DiscountPercentage int `json:"discountPercentage"`
									} `json:"discountSetting"`
								} `json:"promotionalOffers"`
							} `json:"promotionalOffers"`
							UpcomingPromotionalOffers []struct {
								PromotionalOffers []struct {
									DiscountSetting struct {
										DiscountPercentage int `json:"discountPercentage"`
									} `json:"discountSetting"`
								} `json:"promotionalOffers"`
							} `json:"upcomingPromotionalOffers"`
						} `json:"promotions"`
					} `json:"elements"`
				} `json:"searchStore"`
			} `json:"Catalog"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return fmt.Sprintf("❌ Epic查询失败: %v", err)
	}
	games := raw.Data.Catalog.SearchStore.Elements
	freeList := []string{}
	for _, g := range games {
		promotions := g.Promotions
		isFree := false
		offers := promotions.PromotionalOffers
		if len(offers) == 0 && len(promotions.UpcomingPromotionalOffers) > 0 {
			offers = promotions.UpcomingPromotionalOffers
		}
		for _, promo := range offers {
			for _, offer := range promo.PromotionalOffers {
				if offer.DiscountSetting.DiscountPercentage == 0 {
					isFree = true
					break
				}
			}
			if isFree {
				break
			}
		}
		if isFree {
			slug := g.ProductSlug
			if slug == "" {
				slug = g.URLSlug
			}
			link := ""
			if slug != "" {
				link = "https://store.epicgames.com/zh-CN/p/" + slug
			}
			freeList = append(freeList, fmt.Sprintf("🎮 %s\n🔗 %s", g.Title, link))
		}
	}
	if len(freeList) == 0 {
		return "🎮 当前没有免费游戏"
	}
	if len(freeList) > 3 {
		freeList = freeList[:3]
	}
	return "🎮 Epic 喜加一:\n\n" + strings.Join(freeList, "\n")
}

func (b *qqBotServer) getBilibiliHotSearch() string {
	url := "https://app.bilibili.com/x/v2/search/trending/ranking"
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	q := req.URL.Query()
	q.Set("limit", "15")
	req.URL.RawQuery = q.Encode()
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("Referer", "https://www.bilibili.com/")
	resp, err := b.httpClient.Do(req)
	if err != nil {
		return fmt.Sprintf("❌ 错误: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Sprintf("❌ 获取失败 (Code %d)", resp.StatusCode)
	}
	var raw struct {
		Code int `json:"code"`
		Data struct {
			List []struct {
				ShowName string `json:"show_name"`
			} `json:"list"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return fmt.Sprintf("❌ 错误: %v", err)
	}
	if raw.Code != 0 {
		return "❌ 获取失败"
	}
	res := []string{"🔥 B站热搜榜:"}
	for i, item := range raw.Data.List {
		res = append(res, fmt.Sprintf("%d. %s", i+1, item.ShowName))
	}
	return strings.Join(res, "\n")
}

// resolveBilibiliBVFromURL 尝试从任意相关 URL 中解析出 BV 号。
// 支持:
//   - 直接包含 BV 号的链接: https://www.bilibili.com/video/BVxxxxxxxxxxx
//   - B 站短链: https://b23.tv/xxxxxx
func (b *qqBotServer) resolveBilibiliBVFromURL(rawURL string) (string, error) {
	if rawURL == "" {
		return "", fmt.Errorf("空 URL")
	}
	// 1) 链接本身就带 BV 号
	if bv := b.reBV.FindString(rawURL); bv != "" {
		return bv, nil
	}

	// 2) 仅处理 b23.tv 等短链，跟随一次重定向拿到真实地址
	if !strings.Contains(rawURL, "b23.tv") {
		return "", fmt.Errorf("非 B 站短链，且未包含 BV 号")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return "", err
	}
	setCrawlerHeaders(req, "https://www.bilibili.com/")

	resp, err := b.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	// 优先从最终请求 URL 解析（Go 默认已自动跟随重定向）
	finalURL := ""
	if resp.Request != nil && resp.Request.URL != nil {
		finalURL = resp.Request.URL.String()
	}
	if bv := b.reBV.FindString(finalURL); bv != "" {
		return bv, nil
	}

	// 回退使用 Location 头（兼容未自动跟随重定向的情况）
	if loc := resp.Header.Get("Location"); loc != "" {
		if bv := b.reBV.FindString(loc); bv != "" {
			return bv, nil
		}
	}

	return "", fmt.Errorf("未在短链重定向中解析到 BV 号")
}

func (b *qqBotServer) parseBilibiliBV(bvid string) (string, string) {
	if bvid == "" {
		return "", "❌ BV号为空"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, bilibiliAPIBase, nil)
	q := req.URL.Query()
	q.Set("bvid", bvid)
	req.URL.RawQuery = q.Encode()
	setCrawlerHeaders(req, "https://www.bilibili.com/video/"+bvid)
	resp, err := b.httpClient.Do(req)
	if err != nil {
		return "", fmt.Sprintf("❌ 解析错误: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var raw struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Data    struct {
			Title string `json:"title"`
			Pic   string `json:"pic"`
			Owner struct {
				Name string `json:"name"`
			} `json:"owner"`
			Stat struct {
				View int `json:"view"`
				Like int `json:"like"`
				Coin int `json:"coin"`
			} `json:"stat"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		if pic, text := b.parseBilibiliBVFromHTML(bvid); text != "" {
			return pic, text
		}
		return "", fmt.Sprintf("❌ 解析错误: %v", err)
	}
	if raw.Code != 0 {
		if pic, text := b.parseBilibiliBVFromHTML(bvid); text != "" {
			return pic, text
		}
		return "", fmt.Sprintf("❌ 视频失效: %s", raw.Message)
	}
	info := raw.Data
	res := fmt.Sprintf("📺 %s\n👤 UP: %s\n📊 播放: %d  👍 %d  💰 %d\n🔗 https://www.bilibili.com/video/%s",
		info.Title, info.Owner.Name, info.Stat.View, info.Stat.Like, info.Stat.Coin, bvid)
	return info.Pic, res
}

func setCrawlerHeaders(req *http.Request, referer string) {
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.8,*/*;q=0.7")
	req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.7")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Pragma", "no-cache")
	if referer != "" {
		req.Header.Set("Referer", referer)
	}
}

func (b *qqBotServer) parseBilibiliBVFromHTML(bvid string) (string, string) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pageURL := "https://www.bilibili.com/video/" + bvid
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, pageURL, nil)
	setCrawlerHeaders(req, "https://www.bilibili.com/")
	resp, err := b.httpClient.Do(req)
	if err != nil {
		return "", ""
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return "", ""
	}
	html := string(body)
	title := ""
	if m := regexp.MustCompile(`(?is)<meta\s+property="og:title"\s+content="([^"]+)"`).FindStringSubmatch(html); len(m) >= 2 {
		title = htmlUnescapeLite(m[1])
	}
	if title == "" {
		if m := regexp.MustCompile(`(?is)<title>([^<]+)</title>`).FindStringSubmatch(html); len(m) >= 2 {
			title = htmlUnescapeLite(strings.Replace(m[1], "_哔哩哔哩_bilibili", "", 1))
		}
	}
	pic := ""
	if m := regexp.MustCompile(`(?is)<meta\s+property="og:image"\s+content="([^"]+)"`).FindStringSubmatch(html); len(m) >= 2 {
		pic = strings.TrimSpace(htmlUnescapeLite(m[1]))
		if strings.HasPrefix(pic, "//") {
			pic = "https:" + pic
		}
	}
	up := ""
	if m := regexp.MustCompile(`(?is)"name"\s*:\s*"([^"]+)"\s*,\s*"face"`).FindStringSubmatch(html); len(m) >= 2 {
		up = htmlUnescapeLite(m[1])
	}
	if title == "" {
		return "", ""
	}
	if up == "" {
		up = "未知"
	}
	res := fmt.Sprintf("📺 %s\n👤 UP: %s\n🔗 %s", title, up, pageURL)
	return pic, res
}

func htmlUnescapeLite(s string) string {
	repl := strings.NewReplacer("&amp;", "&", "&quot;", "\"", "&#34;", "\"", "&#39;", "'", "&lt;", "<", "&gt;", ">")
	return repl.Replace(s)
}

func (b *qqBotServer) fetchImageBase64(url string, useProxy bool) (string, error) {
	client := b.httpClient
	if useProxy && b.proxyClient != nil {
		client = b.proxyClient
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status %d", resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return "base64://" + base64.StdEncoding.EncodeToString(data), nil
}

func (b *qqBotServer) parseYouTubeVideo(videoID string) (string, string) {
	if videoID == "" {
		return "", "❌ 视频ID为空"
	}
	url := "https://www.youtube.com/watch?v=" + videoID
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := b.proxyClient
	if client == nil {
		client = b.httpClient
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
	req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9")
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Sprintf("❌ 解析异常: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Sprintf("❌ 解析异常: %v", err)
	}
	htmlText := string(body)
	title := "Unknown"
	if m := regexp.MustCompile(`(?i)<title>([^<]*)</title>`).FindStringSubmatch(htmlText); len(m) >= 2 {
		title = strings.TrimSpace(m[1])
	}
	title = strings.Replace(title, " - YouTube", "", 1)
	coverURL := fmt.Sprintf("https://i.ytimg.com/vi/%s/mqdefault.jpg", videoID)
	b64, err := b.fetchImageBase64(coverURL, true)
	if err != nil {
		b64 = coverURL
	}
	res := fmt.Sprintf("🎬 YouTube 视频\n📺 标题: %s\n🔗 链接: %s", title, url)
	return b64, res
}

// ============ Steam / Web 工具 ============

type steamPlayerSummary struct {
	SteamID       string `json:"steamid"`
	PersonaName   string `json:"personaname"`
	PersonaState  int    `json:"personastate"`
	GameExtraInfo string `json:"gameextrainfo"`
	GameID        string `json:"gameid"`
}

func (b *qqBotServer) loadSteamWatch() {
	b.steamMu.Lock()
	defer b.steamMu.Unlock()
	data, err := os.ReadFile(steamWatchFile)
	if err != nil {
		return
	}
	var s steamWatchState
	if err := json.Unmarshal(data, &s); err != nil {
		b.logger.Printf("加载 Steam 监控失败: %v", err)
		return
	}
	if s.Watched == nil {
		s.Watched = make(map[string]*steamWatchEntry)
	}
	b.steamWatch = s.Watched
}

func (b *qqBotServer) saveSteamWatchLocked() {
	data, err := json.MarshalIndent(steamWatchState{Watched: b.steamWatch}, "", "  ")
	if err != nil {
		b.logger.Printf("保存 Steam 监控失败: %v", err)
		return
	}
	tmp := steamWatchFile + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		b.logger.Printf("保存 Steam 监控失败: %v", err)
		return
	}
	if err := os.Rename(tmp, steamWatchFile); err != nil {
		b.logger.Printf("保存 Steam 监控失败: %v", err)
	}
}

func normalizeSteamID64(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	raw = strings.Trim(raw, "<>")
	if m := regexp.MustCompile(`steamcommunity\.com/profiles/(\d{15,20})`).FindStringSubmatch(raw); len(m) >= 2 {
		raw = m[1]
	}
	if m := regexp.MustCompile(`\[U:1:(\d+)\]`).FindStringSubmatch(raw); len(m) >= 2 {
		accountID, _ := strconv.ParseInt(m[1], 10, 64)
		return strconv.FormatInt(76561197960265728+accountID, 10), true
	}
	if regexp.MustCompile(`^\d{15,20}$`).MatchString(raw) {
		return raw, true
	}
	if regexp.MustCompile(`^\d{1,12}$`).MatchString(raw) {
		accountID, _ := strconv.ParseInt(raw, 10, 64)
		return strconv.FormatInt(76561197960265728+accountID, 10), true
	}
	return "", false
}

func extractSteamVanity(raw string) string {
	raw = strings.TrimSpace(raw)
	if m := regexp.MustCompile(`steamcommunity\.com/id/([^/?#\s]+)`).FindStringSubmatch(raw); len(m) >= 2 {
		return m[1]
	}
	if strings.Contains(raw, "/") || strings.Contains(raw, " ") {
		return ""
	}
	if _, ok := normalizeSteamID64(raw); ok {
		return ""
	}
	return raw
}

func (b *qqBotServer) resolveSteamTarget(raw string) (string, error) {
	if id, ok := normalizeSteamID64(raw); ok {
		return id, nil
	}
	vanity := extractSteamVanity(raw)
	if vanity == "" {
		return "", errors.New("无法识别 SteamID64 / 数字好友代码 / Steam 个人资料链接")
	}
	if steamAPIKey == "" {
		return "", errors.New("Steam 未配置 API Key")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	u := strings.TrimRight(steamAPIBase, "/") + "/ISteamUser/ResolveVanityURL/v1/"
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	q := req.URL.Query()
	q.Set("key", steamAPIKey)
	q.Set("vanityurl", vanity)
	req.URL.RawQuery = q.Encode()
	resp, err := b.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var rawResp struct {
		Response struct {
			Success int    `json:"success"`
			SteamID string `json:"steamid"`
			Message string `json:"message"`
		} `json:"response"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rawResp); err != nil {
		return "", err
	}
	if rawResp.Response.Success != 1 || rawResp.Response.SteamID == "" {
		if rawResp.Response.Message != "" {
			return "", errors.New(rawResp.Response.Message)
		}
		return "", errors.New("Steam 自定义链接解析失败")
	}
	return rawResp.Response.SteamID, nil
}

func (b *qqBotServer) fetchSteamSummaries(ids []string) (map[string]steamPlayerSummary, error) {
	if steamAPIKey == "" {
		return nil, errors.New("Steam 未配置 API Key")
	}
	if len(ids) == 0 {
		return map[string]steamPlayerSummary{}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	u := strings.TrimRight(steamAPIBase, "/") + "/ISteamUser/GetPlayerSummaries/v0002/"
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	q := req.URL.Query()
	q.Set("key", steamAPIKey)
	q.Set("steamids", strings.Join(ids, ","))
	req.URL.RawQuery = q.Encode()
	resp, err := b.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var raw struct {
		Response struct {
			Players []steamPlayerSummary `json:"players"`
		} `json:"response"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, err
	}
	out := make(map[string]steamPlayerSummary, len(raw.Response.Players))
	for _, p := range raw.Response.Players {
		out[p.SteamID] = p
	}
	return out, nil
}

func (b *qqBotServer) getSteamFriendsStatus(raw string) string {
	id, err := b.resolveSteamTarget(raw)
	if err != nil {
		return "❌ Steam 好友解析失败: " + err.Error()
	}
	if steamAPIKey == "" {
		return "❌ Steam 未配置 API Key"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	u := strings.TrimRight(steamAPIBase, "/") + "/ISteamUser/GetFriendList/v1/"
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	q := req.URL.Query()
	q.Set("key", steamAPIKey)
	q.Set("steamid", id)
	q.Set("relationship", "friend")
	req.URL.RawQuery = q.Encode()
	resp, err := b.httpClient.Do(req)
	if err != nil {
		return "❌ Steam 好友查询失败: " + err.Error()
	}
	defer resp.Body.Close()
	var rawResp struct {
		FriendsList struct {
			Friends []struct {
				SteamID string `json:"steamid"`
			} `json:"friends"`
		} `json:"friendslist"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rawResp); err != nil {
		return "❌ Steam 好友查询失败: " + err.Error()
	}
	if len(rawResp.FriendsList.Friends) == 0 {
		return "ℹ️ 没有拿到好友列表，可能该账号好友列表未公开"
	}
	ids := make([]string, 0, len(rawResp.FriendsList.Friends))
	for i, f := range rawResp.FriendsList.Friends {
		if i >= 100 {
			break
		}
		ids = append(ids, f.SteamID)
	}
	summaries, err := b.fetchSteamSummaries(ids)
	if err != nil {
		return "❌ Steam 好友状态查询失败: " + err.Error()
	}
	lines := []string{"👥 Steam 好友在线/游戏状态:"}
	count := 0
	for _, sid := range ids {
		p := summaries[sid]
		if p.PersonaState == 0 && p.GameExtraInfo == "" {
			continue
		}
		count++
		status := steamStateName(p.PersonaState)
		if p.GameExtraInfo != "" {
			status += "，正在玩 " + p.GameExtraInfo
		}
		lines = append(lines, fmt.Sprintf("%d. %s (%s)", count, p.PersonaName, status))
		if count >= 30 {
			break
		}
	}
	if count == 0 {
		return "ℹ️ 好友列表里当前没有在线或正在游戏的用户"
	}
	return strings.Join(lines, "\n")
}

func steamStateName(state int) string {
	switch state {
	case 0:
		return "离线"
	case 1:
		return "在线"
	case 2:
		return "忙碌"
	case 3:
		return "离开"
	case 4:
		return "打盹"
	case 5:
		return "想交易"
	case 6:
		return "想玩游戏"
	default:
		return fmt.Sprintf("未知(%d)", state)
	}
}

func (b *qqBotServer) handleSteamWatchCommand(raw string) string {
	id, err := b.resolveSteamTarget(raw)
	if err != nil {
		return "❌ 添加监控失败: " + err.Error()
	}
	summaries, err := b.fetchSteamSummaries([]string{id})
	if err != nil {
		return "❌ 查询 Steam 失败: " + err.Error()
	}
	p := summaries[id]
	name := p.PersonaName
	if name == "" {
		name = id
	}
	b.steamMu.Lock()
	defer b.steamMu.Unlock()
	entry := b.steamWatch[id]
	if entry == nil {
		entry = &steamWatchEntry{SteamID: id}
		b.steamWatch[id] = entry
	}
	entry.Name = name
	entry.LastState = p.PersonaState
	entry.LastGameID = p.GameID
	entry.LastGameName = p.GameExtraInfo
	if p.GameExtraInfo != "" && entry.GameStarted == "" {
		entry.GameStarted = time.Now().Format(time.RFC3339)
	}
	entry.UpdatedAt = time.Now().Format(time.RFC3339)
	b.saveSteamWatchLocked()
	extra := ""
	if p.GameExtraInfo != "" {
		extra = "\n当前游戏: " + p.GameExtraInfo
	}
	return fmt.Sprintf("✅ 已加入 Steam 监控: %s (%s)\n状态: %s%s", name, id, steamStateName(p.PersonaState), extra)
}

func (b *qqBotServer) handleSteamWatchRemoveCommand(raw string) string {
	raw = strings.TrimSpace(raw)
	b.steamMu.Lock()
	defer b.steamMu.Unlock()
	targetID := ""
	if id, ok := normalizeSteamID64(raw); ok {
		targetID = id
	} else {
		for id, entry := range b.steamWatch {
			if strings.EqualFold(entry.Name, raw) || strings.Contains(strings.ToLower(entry.Name), strings.ToLower(raw)) {
				targetID = id
				break
			}
		}
	}
	if targetID == "" {
		return "❌ 未找到要删除的 Steam 监控对象"
	}
	name := b.steamWatch[targetID].Name
	delete(b.steamWatch, targetID)
	b.saveSteamWatchLocked()
	return fmt.Sprintf("✅ 已删除 Steam 监控: %s (%s)", name, targetID)
}

func (b *qqBotServer) handleSteamWatchListCommand() string {
	b.steamMu.RLock()
	defer b.steamMu.RUnlock()
	if len(b.steamWatch) == 0 {
		return "📭 当前没有 Steam 监控对象"
	}
	lines := []string{"🎮 Steam 监控列表:"}
	for _, e := range b.steamWatch {
		status := steamStateName(e.LastState)
		if e.LastGameName != "" {
			status += "，正在玩 " + e.LastGameName
		}
		lines = append(lines, fmt.Sprintf("- %s (%s): %s", e.Name, e.SteamID, status))
	}
	return strings.Join(lines, "\n")
}

func (b *qqBotServer) startSteamMonitor() {
	if steamPollInterval <= 0 {
		return
	}
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		ticker := time.NewTicker(steamPollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				b.pollSteamWatch()
			case <-b.shutdown:
				return
			}
		}
	}()
}

func (b *qqBotServer) pollSteamWatch() {
	if steamAPIKey == "" || len(steamMonitorGroups) == 0 {
		return
	}
	b.steamMu.RLock()
	ids := make([]string, 0, len(b.steamWatch))
	for id := range b.steamWatch {
		ids = append(ids, id)
	}
	b.steamMu.RUnlock()
	if len(ids) == 0 {
		return
	}
	summaries, err := b.fetchSteamSummaries(ids)
	if err != nil {
		b.logger.Printf("Steam 监控轮询失败: %v", err)
		return
	}
	now := time.Now()
	var notices []string
	b.steamMu.Lock()
	for _, id := range ids {
		entry := b.steamWatch[id]
		if entry == nil {
			continue
		}
		p, ok := summaries[id]
		if !ok {
			continue
		}
		oldGame := entry.LastGameName
		oldState := entry.LastState
		if p.PersonaName != "" {
			entry.Name = p.PersonaName
		}
		if p.GameExtraInfo != oldGame {
			if oldGame == "" && p.GameExtraInfo != "" {
				entry.GameStarted = now.Format(time.RFC3339)
				notices = append(notices, fmt.Sprintf("🎮 Steam 监控\n%s 开始游玩: %s", entry.Name, p.GameExtraInfo))
			} else if oldGame != "" && p.GameExtraInfo == "" {
				started, _ := time.Parse(time.RFC3339, entry.GameStarted)
				duration := ""
				if !started.IsZero() {
					duration = fmt.Sprintf("\n本次游玩时长: %s", formatDurationCN(now.Sub(started)))
				}
				notices = append(notices, fmt.Sprintf("🛑 Steam 监控\n%s 已停止游玩: %s%s", entry.Name, oldGame, duration))
				entry.GameStarted = ""
			} else if oldGame != "" && p.GameExtraInfo != "" {
				entry.GameStarted = now.Format(time.RFC3339)
				notices = append(notices, fmt.Sprintf("🔄 Steam 监控\n%s 从 %s 切换到: %s", entry.Name, oldGame, p.GameExtraInfo))
			}
		}
		if oldState != p.PersonaState && p.GameExtraInfo == "" {
			notices = append(notices, fmt.Sprintf("👤 Steam 监控\n%s 状态变更: %s -> %s", entry.Name, steamStateName(oldState), steamStateName(p.PersonaState)))
		}
		entry.LastState = p.PersonaState
		entry.LastGameID = p.GameID
		entry.LastGameName = p.GameExtraInfo
		entry.UpdatedAt = now.Format(time.RFC3339)
	}
	if len(notices) > 0 {
		b.saveSteamWatchLocked()
	}
	b.steamMu.Unlock()
	for _, notice := range notices {
		b.broadcastToSteamGroups(notice)
	}
}

func formatDurationCN(d time.Duration) string {
	if d < 0 {
		d = -d
	}
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	if h > 0 {
		return fmt.Sprintf("%d小时%d分钟", h, m)
	}
	return fmt.Sprintf("%d分钟", m)
}

func (b *qqBotServer) broadcastToSteamGroups(text string) {
	b.clientsMu.RLock()
	clients := make([]*wsClient, 0, len(b.clients))
	for c := range b.clients {
		clients = append(clients, c)
	}
	b.clientsMu.RUnlock()
	for _, gid := range steamMonitorGroups {
		groupID, err := strconv.ParseInt(strings.TrimSpace(gid), 10, 64)
		if err != nil || groupID <= 0 {
			continue
		}
		for _, c := range clients {
			b.sendLongText(c, "group", groupID, text, 0)
		}
	}
}

func (b *qqBotServer) getSteamDiscounts(query string) string {
	query = strings.TrimSpace(query)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if query != "" {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://store.steampowered.com/api/storesearch/", nil)
		q := req.URL.Query()
		q.Set("term", query)
		q.Set("cc", "cn")
		q.Set("l", "schinese")
		req.URL.RawQuery = q.Encode()
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36")
		resp, err := b.httpClient.Do(req)
		if err != nil {
			return "❌ Steam 折扣查询失败: " + err.Error()
		}
		defer resp.Body.Close()
		var raw struct {
			Items []struct {
				Name      string `json:"name"`
				ID        int    `json:"id"`
				TinyImage string `json:"tiny_image"`
				Price     struct {
					Final           int `json:"final"`
					Initial         int `json:"initial"`
					DiscountPercent int `json:"discount_percent"`
				} `json:"price"`
			} `json:"items"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
			return "❌ Steam 折扣查询失败: " + err.Error()
		}
		lines := []string{"🛒 Steam 折扣查询: " + query}
		count := 0
		for _, item := range raw.Items {
			if item.Price.DiscountPercent <= 0 {
				continue
			}
			count++
			lines = append(lines, fmt.Sprintf("%d. %s -%d%% ￥%.2f -> ￥%.2f\nhttps://store.steampowered.com/app/%d", count, item.Name, item.Price.DiscountPercent, float64(item.Price.Initial)/100, float64(item.Price.Final)/100, item.ID))
			if count >= 8 {
				break
			}
		}
		if count == 0 {
			return "ℹ️ 没找到正在打折的相关物品"
		}
		return strings.Join(lines, "\n")
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://store.steampowered.com/api/featuredcategories/", nil)
	q := req.URL.Query()
	q.Set("cc", "cn")
	q.Set("l", "schinese")
	req.URL.RawQuery = q.Encode()
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36")
	resp, err := b.httpClient.Do(req)
	if err != nil {
		return "❌ Steam 折扣查询失败: " + err.Error()
	}
	defer resp.Body.Close()
	var raw struct {
		Specials struct {
			Items []struct {
				Name            string `json:"name"`
				ID              int    `json:"id"`
				FinalPrice      int    `json:"final_price"`
				OriginalPrice   int    `json:"original_price"`
				DiscountPercent int    `json:"discount_percent"`
			} `json:"items"`
		} `json:"specials"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return "❌ Steam 折扣查询失败: " + err.Error()
	}
	lines := []string{"🛒 Steam 当前热门折扣:"}
	for i, item := range raw.Specials.Items {
		if i >= 10 {
			break
		}
		lines = append(lines, fmt.Sprintf("%d. %s -%d%% ￥%.2f -> ￥%.2f\nhttps://store.steampowered.com/app/%d", i+1, item.Name, item.DiscountPercent, float64(item.OriginalPrice)/100, float64(item.FinalPrice)/100, item.ID))
	}
	if len(lines) == 1 {
		return "ℹ️ 当前没有拿到 Steam 热门折扣"
	}
	return strings.Join(lines, "\n")
}

func (b *qqBotServer) captureWebScreenshot(rawURL string) (string, string) {
	rawURL = strings.TrimSpace(rawURL)
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", "❌ 用法: //web https://example.com"
	}
	bin := findChromeBinary()
	if bin == "" {
		return "", "❌ 截图失败: 未找到 Chrome/Chromium，可安装 Google Chrome 或设置 CHROME_BIN"
	}
	tmp, err := os.CreateTemp("", "yaqqbot-web-*.png")
	if err != nil {
		return "", "❌ 截图失败: " + err.Error()
	}
	tmpPath := tmp.Name()
	_ = tmp.Close()
	defer os.Remove(tmpPath)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	args := []string{
		"--headless=new",
		"--disable-gpu",
		"--no-sandbox",
		"--hide-scrollbars",
		"--window-size=1365,900",
		"--screenshot=" + tmpPath,
		rawURL,
	}
	cmd := exec.CommandContext(ctx, bin, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Sprintf("❌ 截图失败: %v %s", err, strings.TrimSpace(string(out)))
	}
	pngData, err := os.ReadFile(tmpPath)
	if err != nil {
		return "", "❌ 截图失败: " + err.Error()
	}
	return "base64://" + base64.StdEncoding.EncodeToString(pngData), "🌐 页面截图: " + rawURL
}

func findChromeBinary() string {
	if v := strings.TrimSpace(os.Getenv("CHROME_BIN")); v != "" {
		return v
	}
	candidates := []string{
		"google-chrome",
		"chromium",
		"chromium-browser",
		"microsoft-edge",
		"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
		"/Applications/Chromium.app/Contents/MacOS/Chromium",
		"/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
	}
	for _, c := range candidates {
		if filepath.IsAbs(c) {
			if st, err := os.Stat(c); err == nil && !st.IsDir() {
				return c
			}
			continue
		}
		if p, err := exec.LookPath(c); err == nil {
			return p
		}
	}
	return ""
}

// ============ AI 调用 ============

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

func (b *qqBotServer) callGPTAPI(messages []chatMessage, imagePaths []string) string {
	if gptAPIKey == "" {
		return "❌ GPT 未配置 API Key"
	}
	apiBase := openaiutil.NormalizeAPIBase(gptAPIBase)
	origin := openaiutil.OriginFromAPIBase(apiBase)

	// 特殊处理：codex-api.packycode.com 明确限制“仅允许官方 Codex CLI 访问”。
	// 这里不尝试伪装/绕过，而是直接调用本机 Codex CLI 来完成对话（需要你已安装并登录/配置）。
	if len(imagePaths) > 0 || strings.Contains(strings.ToLower(apiBase), "codex-api.packycode.com") {
		prompt := buildConversationPrompt(messages, len(imagePaths))
		out, err := codexcli.Exec(context.Background(), prompt, codexcli.ExecOptions{
			Bin:     envOrDefault("CODEX_CLI_BIN", "codex"),
			Model:   gptModel,
			APIBase: apiBase,
			APIKey:  gptAPIKey,
			EnvKey:  "GPT_API_KEY",
			WireAPI: envOrDefault("CODEX_WIRE_API", "responses"),
			Images:  imagePaths,
			SkipGitRepoCheck: func() *bool {
				v := envBoolOrDefault("CODEX_SKIP_GIT_REPO_CHECK", true)
				return &v
			}(),
			Timeout: 180 * time.Second,
		})
		if err != nil {
			return "❌ Codex CLI 调用失败: " + err.Error() + "\n" +
				"提示：请先安装官方 Codex CLI，并确保能在命令行直接运行 `codex exec`。"
		}
		return out
	}

	// 优先尝试 OpenAI 新版 /v1/responses（Codex CLI、gpt-5.* 等模型通常走该接口）。
	{
		url := apiBase + "/responses"
		reqBody := map[string]any{
			"model":             gptModel,
			"input":             messages,
			"max_output_tokens": 4000,
		}
		data, _ := json.Marshal(reqBody)
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
		req.Header.Set("Authorization", "Bearer "+gptAPIKey)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36")
		req.Header.Set("Origin", origin)
		req.Header.Set("Referer", origin+"/")

		resp, err := b.httpClient.Do(req)
		if err != nil {
			return fmt.Sprintf("❌ GPT 调用失败: %v", err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode == http.StatusOK {
			if out := openaiutil.ExtractResponsesOutputText(body); out != "" {
				return out
			}
			// 兼容实现差异：如果解析不到 output_text，就把原始响应作为错误回显，方便定位。
			return "❌ GPT 返回结构无法解析（/responses）"
		}

		// 如果服务端不支持 /responses，则回退到 /chat/completions（兼容 OpenAI-compat 接口）。
		// 常见不支持情况：404 / 405 / 未实现；其他错误则直接返回，避免掩盖真实问题。
		if resp.StatusCode != http.StatusNotFound && resp.StatusCode != http.StatusMethodNotAllowed {
			errText := strings.TrimSpace(string(body))
			// 这个报错通常意味着 API Base 填成了 /v1/codex（Codex CLI 专用），或者指向了仅允许 Codex CLI 的代理。
			if strings.Contains(errText, "only accessible via the official Codex CLI") {
				// 说明：
				// - codex-api.packycode.com 往往是给 Codex CLI 用的专用入口（可能会拒绝普通 HTTP 客户端）
				// - 想走 OpenAI 官方：用 https://api.openai.com/v1
				// - 想走 Packy 中转：通常应使用其对外的 OpenAI 兼容 /v1 根地址，而不是 codex-api 专用入口
				msg := "❌ GPT 端点权限不足：该接口仅允许官方 Codex CLI 访问。\n"
				if strings.Contains(strings.ToLower(gptAPIBase), "codex-api.packycode.com") {
					msg += "你当前配置的是 codex-api.packycode.com（Codex CLI 专用）。\n" +
						"如果你要用 Packy 中转，请把 GPT_API_BASE / gpt_api_base 改为 https://www.packyapi.com/v1 再试。\n"
				}
				msg += "如果你要直连 OpenAI 官方，请改为 https://api.openai.com/v1（不要带 /codex）。"
				return msg
			}
			return fmt.Sprintf("GPT API Error: %d %s", resp.StatusCode, errText)
		}
	}

	// 回退：旧版 /v1/chat/completions
	{
		url := apiBase + "/chat/completions"
		reqBody := map[string]any{
			"model":      gptModel,
			"messages":   messages,
			"max_tokens": 4000,
		}
		data, _ := json.Marshal(reqBody)
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
		req.Header.Set("Authorization", "Bearer "+gptAPIKey)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36")
		req.Header.Set("Origin", origin)
		req.Header.Set("Referer", origin+"/")

		resp, err := b.httpClient.Do(req)
		if err != nil {
			return fmt.Sprintf("❌ GPT 调用失败: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Sprintf("GPT API Error: %d %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		var raw struct {
			Choices []struct {
				Message chatMessage `json:"message"`
			} `json:"choices"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
			return fmt.Sprintf("❌ GPT 调用失败: %v", err)
		}
		if len(raw.Choices) == 0 {
			return "❌ GPT 返回为空"
		}
		return raw.Choices[0].Message.Content
	}
}

// buildConversationPrompt 把 messages 组装成适合“纯对话”的文本提示，供 codex exec 使用。
// 约束：不引导其修改文件或执行命令，避免产生副作用。
func buildConversationPrompt(messages []chatMessage, imageCount int) string {
	var b strings.Builder
	if imageCount > 0 {
		b.WriteString(fmt.Sprintf("你是聊天助手。你将收到 %d 张图片作为附件（由系统通过 --image 提供）。请结合图片内容与对话历史回答。\n", imageCount))
	}
	b.WriteString("请不要执行任何命令、不要读写文件、不要修改仓库，只需要回答用户的最后一个问题。\n\n")
	for _, m := range messages {
		role := strings.TrimSpace(m.Role)
		content := strings.TrimSpace(m.Content)
		if content == "" {
			continue
		}
		switch role {
		case "system":
			b.WriteString("【系统】")
		case "user":
			b.WriteString("【用户】")
		case "assistant":
			b.WriteString("【助手】")
		default:
			b.WriteString("【" + role + "】")
		}
		b.WriteString(content)
		b.WriteString("\n\n")
	}
	b.WriteString("请直接输出回答正文，不要加多余前后缀。")
	return b.String()
}

func (b *qqBotServer) callGrokAPI(messages []chatMessage) string {
	if grokAPIKey == "" {
		return "❌ Grok 未配置 API Key"
	}
	url := strings.TrimRight(grokAPIBase, "/") + "/chat/completions"
	reqBody := map[string]any{
		"model":      grokModel,
		"messages":   messages,
		"max_tokens": 2000,
	}
	data, _ := json.Marshal(reqBody)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(data)))
	req.Header.Set("Authorization", "Bearer "+grokAPIKey)
	req.Header.Set("Content-Type", "application/json")
	client := b.proxyClient
	if client == nil {
		client = b.httpClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Sprintf("❌ Grok Error: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Sprintf("Grok Error: %d", resp.StatusCode)
	}
	var raw struct {
		Choices []struct {
			Message chatMessage `json:"message"`
		} `json:"choices"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return fmt.Sprintf("❌ Grok Error: %v", err)
	}
	if len(raw.Choices) == 0 {
		return "❌ Grok 返回为空"
	}
	return raw.Choices[0].Message.Content
}

func (b *qqBotServer) callDeepSeekAPI(messages []chatMessage) string {
	if deepSeekAPIKey == "" {
		return "❌ DeepSeek 未配置 API Key"
	}
	url := strings.TrimRight(deepSeekAPIBase, "/") + "/chat/completions"
	reqBody := map[string]any{
		"model":       deepSeekModel,
		"messages":    messages,
		"temperature": 0.7,
		"max_tokens":  4000,
	}
	data, _ := json.Marshal(reqBody)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	req.Header.Set("Authorization", "Bearer "+deepSeekAPIKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := b.httpClient.Do(req)
	if err != nil {
		return fmt.Sprintf("❌ DeepSeek 调用失败: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Sprintf("DeepSeek API Error: %d %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var raw struct {
		Choices []struct {
			Message chatMessage `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return fmt.Sprintf("❌ DeepSeek 解析失败: %v", err)
	}
	if len(raw.Choices) == 0 {
		return "❌ DeepSeek 返回为空"
	}
	return raw.Choices[0].Message.Content
}

// 简单 Claude API 调用（兼容官方 /v1/messages 风格）
func (b *qqBotServer) callClaudeAPI(systemPrompt string, messages []chatMessage) string {
	if claudeAPIKey == "" {
		return "❌ Claude 未配置 API Key"
	}
	type contentBlock struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	type claudeMsg struct {
		Role    string         `json:"role"`
		Content []contentBlock `json:"content"`
	}
	var claudeMsgs []claudeMsg
	for _, m := range messages {
		if m.Role == "system" {
			continue
		}
		claudeMsgs = append(claudeMsgs, claudeMsg{
			Role:    m.Role,
			Content: []contentBlock{{Type: "text", Text: m.Content}},
		})
	}
	url := strings.TrimRight(claudeAPIBase, "/") + "/v1/messages"
	reqBody := map[string]any{
		"model":      claudeModel,
		"max_tokens": 2000,
		"system":     systemPrompt,
		"messages":   claudeMsgs,
	}
	data, _ := json.Marshal(reqBody)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(data)))
	req.Header.Set("x-api-key", claudeAPIKey)
	req.Header.Set("anthropic-version", "2023-06-01")
	req.Header.Set("Content-Type", "application/json")
	resp, err := b.httpClient.Do(req)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Sprintf("Error: %d %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var raw struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return fmt.Sprintf("Error: %v", err)
	}
	if len(raw.Content) == 0 {
		return "Error: empty response"
	}
	return raw.Content[0].Text
}

func (b *qqBotServer) callGeminiImage(prompt string, width, height int) (imgBase64 string, text string, err error) {
	if strings.TrimSpace(geminiAPIKey) == "" {
		return "", "", errors.New("Gemini 未配置 API Key（GEMINI_API_KEY）")
	}
	apiBase := strings.TrimRight(strings.TrimSpace(geminiAPIBase), "/")
	if apiBase == "" {
		apiBase = "https://generativelanguage.googleapis.com/v1beta"
	}
	model := strings.TrimSpace(geminiImageModel)
	if model == "" {
		model = "gemini-2.5-flash-image"
	}
	if width > 0 && height > 0 {
		prompt = fmt.Sprintf("%s\n\n请生成一张 %dx%d 像素、宽高比 %.4f:1 的图片。", prompt, width, height, float64(width)/float64(height))
	}

	url := fmt.Sprintf("%s/models/%s:generateContent", apiBase, model)
	reqBody := map[string]any{
		"contents": []map[string]any{
			{
				"parts": []map[string]any{
					{"text": prompt},
				},
			},
		},
	}
	data, _ := json.Marshal(reqBody)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return "", "", err
	}
	req.Header.Set("x-goog-api-key", geminiAPIKey)
	req.Header.Set("Content-Type", "application/json")

	client := b.proxyClient
	if client == nil {
		client = b.httpClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("Gemini API Error: %d %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	// 兼容 inlineData / inline_data 两种字段命名
	type inlineData struct {
		MimeType string `json:"mimeType"`
		Data     string `json:"data"`
	}
	type part struct {
		Text        string      `json:"text"`
		InlineData  *inlineData `json:"inlineData"`
		InlineData2 *inlineData `json:"inline_data"`
	}
	var raw struct {
		Candidates []struct {
			Content struct {
				Parts []part `json:"parts"`
			} `json:"content"`
		} `json:"candidates"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return "", "", err
	}
	var texts []string
	img := ""
	imgMime := ""
	for _, c := range raw.Candidates {
		for _, p := range c.Content.Parts {
			if t := strings.TrimSpace(p.Text); t != "" {
				texts = append(texts, t)
			}
			id := p.InlineData
			if id == nil {
				id = p.InlineData2
			}
			if img == "" && id != nil && strings.TrimSpace(id.Data) != "" {
				// CQ:image 支持 base64:// 形式
				img = "base64://" + strings.TrimSpace(id.Data)
				imgMime = strings.TrimSpace(id.MimeType)
			}
		}
	}
	if len(texts) > 0 {
		text = strings.Join(texts, "\n")
	}
	if img == "" {
		// 允许“只返回文字”的情况
		if strings.TrimSpace(text) != "" {
			return "", text, nil
		}
		return "", "", errors.New("Gemini 未返回图片/文字数据")
	}
	if width > 0 && height > 0 {
		resized, err := resizeBase64ImageForCQ(img, imgMime, width, height)
		if err != nil {
			return "", "", fmt.Errorf("图片已生成，但调整尺寸失败: %w", err)
		}
		img = resized
	}
	return img, text, nil
}

func parseImageCommand(raw string) (prompt string, width int, height int, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", 0, 0, nil
	}
	re := regexp.MustCompile(`(?i)(?:^|\s)(?:--?size[=\s]+)?(\d{2,4})\s*[x×]\s*(\d{2,4})(?:\s|$)`)
	m := re.FindStringSubmatchIndex(raw)
	if len(m) >= 6 {
		wStr := raw[m[2]:m[3]]
		hStr := raw[m[4]:m[5]]
		w, _ := strconv.Atoi(wStr)
		h, _ := strconv.Atoi(hStr)
		if w < 64 || h < 64 || w > 4096 || h > 4096 {
			return "", 0, 0, fmt.Errorf("尺寸范围应为 64x64 到 4096x4096")
		}
		raw = strings.TrimSpace(raw[:m[0]] + " " + raw[m[1]:])
		return raw, w, h, nil
	}
	return raw, 0, 0, nil
}

func resizeBase64ImageForCQ(cqFile, mimeType string, width, height int) (string, error) {
	b64 := strings.TrimPrefix(strings.TrimSpace(cqFile), "base64://")
	data, err := decodeBase64Any(b64)
	if err != nil {
		return "", err
	}
	src, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return "", err
	}
	dst := image.NewRGBA(image.Rect(0, 0, width, height))
	sb := src.Bounds()
	for y := 0; y < height; y++ {
		sy := sb.Min.Y + y*sb.Dy()/height
		for x := 0; x < width; x++ {
			sx := sb.Min.X + x*sb.Dx()/width
			dst.Set(x, y, src.At(sx, sy))
		}
	}
	var buf bytes.Buffer
	if strings.Contains(strings.ToLower(mimeType), "jpeg") || strings.Contains(strings.ToLower(mimeType), "jpg") {
		if err := jpeg.Encode(&buf, dst, &jpeg.Options{Quality: 92}); err != nil {
			return "", err
		}
	} else {
		if err := png.Encode(&buf, dst); err != nil {
			return "", err
		}
	}
	return "base64://" + base64.StdEncoding.EncodeToString(buf.Bytes()), nil
}

// ============ 消息处理 ============

func (b *qqBotServer) stripCQCodes(message string) string {
	if message == "" {
		return ""
	}
	return strings.TrimSpace(b.reCQ.ReplaceAllString(message, ""))
}

// saveIncomingImages 会从 raw CQ message 中提取图片段并保存到本机临时目录，
// 返回可供 Codex CLI `--image` 使用的绝对路径列表。
func (b *qqBotServer) saveIncomingImages(rawMsg string) ([]string, func()) {
	matches := b.reCQImage.FindAllStringSubmatch(rawMsg, -1)
	if len(matches) == 0 {
		return nil, func() {}
	}

	var paths []string
	var created []string

	for _, m := range matches {
		if len(m) < 2 {
			continue
		}
		params := parseCQKVParams(m[1])
		srcURL := strings.TrimSpace(params["url"])
		file := strings.TrimSpace(params["file"])

		switch {
		case strings.HasPrefix(srcURL, "http://") || strings.HasPrefix(srcURL, "https://"):
			p, err := b.downloadImageToTemp(srcURL)
			if err != nil {
				b.logger.Printf("下载图片失败: %v", err)
				continue
			}
			paths = append(paths, p)
			created = append(created, p)
		case strings.HasPrefix(file, "base64://"):
			p, err := writeBase64ImageToTemp(file)
			if err != nil {
				b.logger.Printf("保存 base64 图片失败: %v", err)
				continue
			}
			paths = append(paths, p)
			created = append(created, p)
		default:
			// 兜底：若 file 本身就是本机绝对路径且存在，则直接传给 codex。
			if filepath.IsAbs(file) {
				if st, err := os.Stat(file); err == nil && !st.IsDir() {
					paths = append(paths, file)
				}
			}
		}
	}

	cleanup := func() {
		for _, p := range created {
			_ = os.Remove(p)
		}
	}
	return paths, cleanup
}

func parseCQKVParams(s string) map[string]string {
	out := make(map[string]string)
	parts := strings.Split(s, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		k, v, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		out[strings.TrimSpace(k)] = cqUnescape(strings.TrimSpace(v))
	}
	return out
}

// cqUnescape 实现 go-cqhttp/OneBot v11 CQ 码转义的逆变换：
// &amp; &#91; &#93; &#44;
func cqUnescape(s string) string {
	s = strings.ReplaceAll(s, "&#44;", ",")
	s = strings.ReplaceAll(s, "&#91;", "[")
	s = strings.ReplaceAll(s, "&#93;", "]")
	s = strings.ReplaceAll(s, "&amp;", "&")
	return s
}

func imageExtFromContentType(ct string) string {
	ct = strings.ToLower(strings.TrimSpace(ct))
	if idx := strings.Index(ct, ";"); idx != -1 {
		ct = strings.TrimSpace(ct[:idx])
	}
	switch ct {
	case "image/jpeg":
		return ".jpg"
	case "image/png":
		return ".png"
	case "image/gif":
		return ".gif"
	case "image/webp":
		return ".webp"
	case "image/bmp":
		return ".bmp"
	default:
		return ".img"
	}
}

func writeBytesToTempImage(data []byte, ext string) (string, error) {
	if ext == "" || !strings.HasPrefix(ext, ".") {
		ext = ".img"
	}
	dir := filepath.Join(os.TempDir(), "yaqqbot-images")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	f, err := os.CreateTemp(dir, "qqimg-*"+ext)
	if err != nil {
		return "", err
	}
	defer f.Close()
	if _, err := f.Write(data); err != nil {
		_ = os.Remove(f.Name())
		return "", err
	}
	return f.Name(), nil
}

func decodeBase64Any(s string) ([]byte, error) {
	// 常见：StdEncoding / RawStdEncoding / URLEncoding
	if b, err := base64.StdEncoding.DecodeString(s); err == nil {
		return b, nil
	}
	if b, err := base64.RawStdEncoding.DecodeString(s); err == nil {
		return b, nil
	}
	if b, err := base64.URLEncoding.DecodeString(s); err == nil {
		return b, nil
	}
	return base64.RawURLEncoding.DecodeString(s)
}

func writeBase64ImageToTemp(fileField string) (string, error) {
	b64 := strings.TrimPrefix(fileField, "base64://")
	data, err := decodeBase64Any(b64)
	if err != nil {
		return "", err
	}
	ct := http.DetectContentType(data)
	return writeBytesToTempImage(data, imageExtFromContentType(ct))
}

func (b *qqBotServer) downloadImageToTemp(srcURL string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srcURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")

	client := b.proxyClient
	if client == nil {
		client = b.httpClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status=%d", resp.StatusCode)
	}
	// 避免意外大文件拖垮进程
	data, err := io.ReadAll(io.LimitReader(resp.Body, 20<<20))
	if err != nil {
		return "", err
	}
	ext := imageExtFromContentType(resp.Header.Get("Content-Type"))
	if ext == ".img" {
		// 兜底：按内容嗅探
		ext = imageExtFromContentType(http.DetectContentType(data))
	}
	return writeBytesToTempImage(data, ext)
}

func (b *qqBotServer) extractReplyMessageID(rawMsg string) (int64, bool) {
	m := b.reCQReply.FindStringSubmatch(rawMsg)
	if len(m) < 2 {
		return 0, false
	}
	params := parseCQKVParams(m[1])
	idStr := strings.TrimSpace(params["id"])
	if idStr == "" {
		return 0, false
	}
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil || id <= 0 {
		return 0, false
	}
	return id, true
}

// saveIncomingImagesFromOneBotMessage 支持从 get_msg 的 message 字段中提取图片。
// message 可能是 CQ 字符串，也可能是 OneBot 消息段数组（[]）。
func (b *qqBotServer) saveIncomingImagesFromOneBotMessage(message any) ([]string, func()) {
	switch v := message.(type) {
	case nil:
		return nil, func() {}
	case string:
		return b.saveIncomingImages(v)
	case []any:
		var paths []string
		var created []string

		for _, segAny := range v {
			seg, ok := segAny.(map[string]any)
			if !ok {
				continue
			}
			typ, _ := seg["type"].(string)
			if typ != "image" && typ != "mface" {
				continue
			}
			data, ok := seg["data"].(map[string]any)
			if !ok {
				continue
			}
			urlStr, _ := data["url"].(string)
			fileStr, _ := data["file"].(string)
			urlStr = strings.TrimSpace(cqUnescape(urlStr))
			fileStr = strings.TrimSpace(cqUnescape(fileStr))

			switch {
			case strings.HasPrefix(urlStr, "http://") || strings.HasPrefix(urlStr, "https://"):
				p, err := b.downloadImageToTemp(urlStr)
				if err != nil {
					continue
				}
				paths = append(paths, p)
				created = append(created, p)
			case strings.HasPrefix(fileStr, "base64://"):
				p, err := writeBase64ImageToTemp(fileStr)
				if err != nil {
					continue
				}
				paths = append(paths, p)
				created = append(created, p)
			default:
				if filepath.IsAbs(fileStr) {
					if st, err := os.Stat(fileStr); err == nil && !st.IsDir() {
						paths = append(paths, fileStr)
					}
				}
			}
		}

		cleanup := func() {
			for _, p := range created {
				_ = os.Remove(p)
			}
		}
		return paths, cleanup
	default:
		// 兜底：尝试把 message 序列化成字符串再走 CQ 解析
		if bts, err := json.Marshal(v); err == nil {
			return b.saveIncomingImages(string(bts))
		}
		return nil, func() {}
	}
}

func splitTextBySize(text string, size int) []string {
	if size <= 0 || len(text) <= size {
		return []string{text}
	}
	var parts []string
	for start := 0; start < len(text); start += size {
		end := start + size
		if end > len(text) {
			end = len(text)
		}
		parts = append(parts, text[start:end])
	}
	return parts
}

func (b *qqBotServer) sendForwardMessage(client *wsClient, messageType string, targetID int64, text string, selfID int64) error {
	if messageType != "group" && messageType != "private" {
		return errors.New("unsupported message type")
	}
	if selfID == 0 {
		selfID = 10000
	}
	nodes := make([]map[string]any, 0)
	for i, part := range splitTextBySize(text, 1800) {
		name := "YaqqBot"
		if i > 0 {
			name = fmt.Sprintf("YaqqBot %d", i+1)
		}
		nodes = append(nodes, map[string]any{
			"type": "node",
			"data": map[string]any{
				"name":    name,
				"uin":     strconv.FormatInt(selfID, 10),
				"content": part,
			},
		})
	}
	action := "send_group_forward_msg"
	params := map[string]any{
		"group_id": targetID,
		"messages": nodes,
	}
	if messageType == "private" {
		action = "send_private_forward_msg"
		params = map[string]any{
			"user_id":  targetID,
			"messages": nodes,
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := client.Call(ctx, action, params)
	return err
}

func (b *qqBotServer) sendLongText(client *wsClient, messageType string, targetID int64, text string, selfID int64) {
	if text == "" {
		return
	}
	if longForwardThreshold > 0 && len(text) > longForwardThreshold {
		if err := b.sendForwardMessage(client, messageType, targetID, text, selfID); err == nil {
			return
		} else {
			b.logger.Printf("合并转发失败，回退分段发送: %v", err)
		}
	}
	for start := 0; start < len(text); start += messageChunkSize {
		end := start + messageChunkSize
		if end > len(text) {
			end = len(text)
		}
		part := text[start:end]
		payload := map[string]any{
			"action": "send_group_msg",
			"params": map[string]any{
				"group_id": targetID,
				"message":  part,
			},
		}
		if messageType == "private" {
			payload["action"] = "send_private_msg"
			payload["params"] = map[string]any{
				"user_id": targetID,
				"message": part,
			}
		}
		if err := client.WriteJSON(payload); err != nil {
			b.logger.Printf("发送消息失败: %v", err)
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (b *qqBotServer) registerClient(client *wsClient) {
	b.clientsMu.Lock()
	defer b.clientsMu.Unlock()
	b.clients[client] = struct{}{}
}

func (b *qqBotServer) unregisterClient(client *wsClient) {
	b.clientsMu.Lock()
	defer b.clientsMu.Unlock()
	delete(b.clients, client)
}

func (b *qqBotServer) handleBotEvent(payload []byte) {
	var evt cqMessage
	if err := json.Unmarshal(payload, &evt); err != nil {
		return
	}
	groupID := ""
	if evt.GroupID != 0 {
		groupID = strconv.FormatInt(evt.GroupID, 10)
	}
	switch evt.PostType {
	case "notice":
		userID := strconv.FormatInt(evt.UserID, 10)
		targetID := strconv.FormatInt(evt.TargetID, 10)
		operatorID := strconv.FormatInt(evt.OperatorID, 10)
		var detail string
		switch evt.NoticeType {
		case "notify":
			if evt.SubType == "poke" {
				detail = fmt.Sprintf("在群 %s 戳了 %s", groupID, targetID)
			} else {
				detail = fmt.Sprintf("notify/%s target=%s operator=%s", evt.SubType, targetID, operatorID)
			}
		case "group_increase":
			detail = fmt.Sprintf("加入群 %s，操作者 %s", groupID, operatorID)
		case "group_decrease":
			detail = fmt.Sprintf("离开群 %s，操作者 %s，类型 %s", groupID, operatorID, evt.SubType)
		case "group_admin", "group_ban", "group_upload", "group_recall", "friend_recall", "friend_add":
			detail = fmt.Sprintf("%s/%s group=%s target=%s operator=%s message=%d", evt.NoticeType, evt.SubType, groupID, targetID, operatorID, evt.MessageID)
		default:
			detail = fmt.Sprintf("%s/%s group=%s target=%s operator=%s", evt.NoticeType, evt.SubType, groupID, targetID, operatorID)
		}
		if userID != "" && userID != "0" {
			b.appendUserEvent(userID, groupID, "qq_notice", detail)
		}
		if targetID != "" && targetID != "0" && targetID != userID {
			b.appendUserEvent(targetID, groupID, "qq_notice", detail)
		}
	case "request":
		userID := strconv.FormatInt(evt.UserID, 10)
		detail := fmt.Sprintf("%s/%s group=%s comment=%s", evt.RequestType, evt.SubType, groupID, evt.Comment)
		b.appendUserEvent(userID, groupID, "qq_request", detail)
	}
}

func compactChatMessages(messages []chatMessage, maxChars int) []chatMessage {
	if maxChars <= 0 {
		return messages
	}
	total := 0
	for _, m := range messages {
		total += len(m.Content)
	}
	if total <= maxChars {
		return messages
	}
	if len(messages) <= 4 {
		return messages
	}
	system := messages[0]
	tail := make([]chatMessage, 0)
	tailChars := 0
	for i := len(messages) - 1; i >= 1; i-- {
		c := len(messages[i].Content)
		if len(tail) >= 6 && tailChars+c > maxChars*2/3 {
			break
		}
		tail = append(tail, messages[i])
		tailChars += c
	}
	for i, j := 0, len(tail)-1; i < j; i, j = i+1, j-1 {
		tail[i], tail[j] = tail[j], tail[i]
	}
	omitted := len(messages) - 1 - len(tail)
	summary := chatMessage{
		Role:    "system",
		Content: fmt.Sprintf("较早的 %d 条历史已被折叠，以控制上下文长度。请优先依据最近消息和用户长期事件记忆回答。", omitted),
	}
	out := []chatMessage{system, summary}
	out = append(out, tail...)
	return out
}

func (b *qqBotServer) processSingleMessage(client *wsClient, payload []byte) {
	var msg cqMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		return
	}
	if msg.PostType != "message" {
		b.handleBotEvent(payload)
		return
	}
	msgType := msg.MessageType
	userID := strconv.FormatInt(msg.UserID, 10)
	groupID := ""
	if msgType == "group" && msg.GroupID != 0 {
		groupID = strconv.FormatInt(msg.GroupID, 10)
	}
	rawMsg := msg.RawMessage
	selfID := strconv.FormatInt(msg.SelfID, 10)

	// 白名单过滤
	if msgType == "private" {
		if _, ok := b.allowedUsers[userID]; !ok {
			return
		}
	}
	if msgType == "group" && len(b.allowedGroups) > 0 {
		if _, ok := b.allowedGroups[groupID]; !ok {
			return
		}
	}

	cleanMsg := b.stripCQCodes(rawMsg)
	commandMsg := cleanMsg
	if strings.HasPrefix(commandMsg, "//") {
		commandMsg = commandMsg[1:]
	}
	isAtMe := msgType == "group" && strings.Contains(rawMsg, "[CQ:at,qq="+selfID+"]")
	hasImage := b.reCQImage.MatchString(rawMsg)
	replyMsgID, hasReply := b.extractReplyMessageID(rawMsg)

	var responseText string
	var responseImg string

	// 小程序/分享卡片消息 (B站)
	if strings.HasPrefix(rawMsg, "[CQ:json,data=") && (strings.Contains(rawMsg, "哔哩哔哩") || strings.Contains(rawMsg, "b23.tv")) {
		if msgType != "group" || b.isGroupBVEnabled(groupID) {
			if m := regexp.MustCompile(`(http[^\"]+)`).FindStringSubmatch(rawMsg); len(m) >= 2 {
				rawURL := m[1]
				if bv, err := b.resolveBilibiliBVFromURL(rawURL); err == nil && bv != "" {
					responseImg, responseText = b.parseBilibiliBV(bv)
				}
			}
		}
	}

	// 指令处理
	if responseText == "" {
		switch {
		case strings.HasPrefix(commandMsg, "/help"):
			responseText = "🤖 帮助:\n" +
				"/ct [问题]           - 问答\n" +
				"/deepseek [问题]     - 使用 DeepSeek agent\n" +
				"/img [尺寸] [提示词] - Gemini 生成图片，如 /img 1024x768 赛博城市\n" +
				"//web [URL]          - 无头浏览器打开网页并截图\n" +
				"//whatch [SteamID/好友代码/自定义名/链接] - 加入 Steam 监控\n" +
				"//whatchrm [名字/SteamID] - 删除 Steam 监控\n" +
				"//list               - 查看 Steam 监控列表\n" +
				"//friends [SteamID/链接] - 查看公开好友在线/游戏状态\n" +
				"//buy [关键词]       - 查询 Steam 折扣，不填显示热门折扣\n" +
				"/ping [域名]         - 代理测速\n" +
				"/nginx ...           - 管理 Nginx stream/远程转发\n" +
				"  /nginx list        - 列出当前配置\n" +
				"  /nginx add ...     - 新增/更新转发\n" +
				"  /nginx rm 名字     - 删除转发\n" +
				"  /nginx -t          - 测试 nginx 配置语法\n" +
				"  /nginx mkdir 名称  - 初始化/选择配置文件\n" +
				"  /nginx set 名称    - 切换默认被控服务器\n" +
				"  /nginx server ...  - 管理被控服务器\n" +
				"/set [模型]          - 个人模型\n" +
				"/setall [模型]       - 群模型\n" +
				"/clear               - 清除记忆\n" +
				"/天气 [城市]\n" +
				"/rs                  - B站热搜\n" +
				"/epic                - Epic 喜加一\n" +
				"/bv [BV号/on/off]"
		case strings.HasPrefix(commandMsg, "/ping "):
			responseText = b.pingViaProxy(strings.TrimSpace(commandMsg[6:]))
		case strings.HasPrefix(commandMsg, "/img "):
			p, w, h, err := parseImageCommand(strings.TrimSpace(commandMsg[5:]))
			if err != nil {
				responseText = "❌ " + err.Error()
				break
			}
			if p == "" {
				responseText = "❌ 用法: /img [可选尺寸如 1024x768] 你的提示词"
				break
			}
			img, txt, err := b.callGeminiImage(p, w, h)
			if err != nil {
				responseText = "❌ Gemini 生成失败: " + err.Error()
			} else {
				responseImg = img
				// 支持 Gemini 同时返回的文本说明；有的响应可能仅返回文字。
				responseText = strings.TrimSpace(txt)
			}
		case strings.HasPrefix(commandMsg, "/web "):
			responseImg, responseText = b.captureWebScreenshot(strings.TrimSpace(commandMsg[5:]))
		case strings.HasPrefix(commandMsg, "/whatch "):
			responseText = b.handleSteamWatchCommand(strings.TrimSpace(commandMsg[len("/whatch "):]))
		case strings.HasPrefix(commandMsg, "/watch "):
			responseText = b.handleSteamWatchCommand(strings.TrimSpace(commandMsg[len("/watch "):]))
		case strings.HasPrefix(commandMsg, "/whatchrm "):
			responseText = b.handleSteamWatchRemoveCommand(strings.TrimSpace(commandMsg[len("/whatchrm "):]))
		case strings.HasPrefix(commandMsg, "/watchrm "):
			responseText = b.handleSteamWatchRemoveCommand(strings.TrimSpace(commandMsg[len("/watchrm "):]))
		case strings.TrimSpace(commandMsg) == "/list":
			responseText = b.handleSteamWatchListCommand()
		case strings.HasPrefix(commandMsg, "/friends "):
			responseText = b.getSteamFriendsStatus(strings.TrimSpace(commandMsg[len("/friends "):]))
		case strings.HasPrefix(commandMsg, "/buy"):
			responseText = b.getSteamDiscounts(strings.TrimSpace(commandMsg[len("/buy"):]))
		case strings.HasPrefix(commandMsg, "/nginx"):
			args := strings.TrimSpace(commandMsg[len("/nginx"):])
			responseText = b.handleNginxCommand(args, userID, msgType)
		case strings.HasPrefix(commandMsg, "/天气 "):
			responseText = b.getWeather(strings.TrimSpace(commandMsg[4:]))
		case strings.HasPrefix(commandMsg, "/rs"):
			responseText = b.getBilibiliHotSearch()
		case strings.HasPrefix(commandMsg, "/epic"):
			responseText = b.getEpicFreeGames()
		case strings.HasPrefix(commandMsg, "/set "):
			model := strings.ToLower(strings.TrimSpace(commandMsg[5:]))
			if b.setUserModel(userID, model) {
				responseText = "✅ 个人模型: " + model
			} else {
				responseText = "❌ 未知模型"
			}
		case strings.HasPrefix(commandMsg, "/setall ") && msgType == "group":
			model := strings.ToLower(strings.TrimSpace(commandMsg[8:]))
			if b.setGroupDefaultModel(groupID, model) {
				responseText = "✅ 群默认模型: " + model
			} else {
				responseText = "❌ 未知模型"
			}
		case strings.TrimSpace(commandMsg) == "/clear":
			if b.clearUserMemory(userID) {
				responseText = "🧹 记忆已清除"
			} else {
				responseText = "ℹ️ 无记忆可清除"
			}
		case strings.HasPrefix(commandMsg, "/bv "):
			arg := strings.TrimSpace(commandMsg[4:])
			if arg == "on" {
				b.setGroupBVEnabled(groupID, true)
				responseText = "✅ BV解析开启"
			} else if arg == "off" {
				b.setGroupBVEnabled(groupID, false)
				responseText = "🚫 BV解析关闭"
			} else {
				responseImg, responseText = b.parseBilibiliBV(arg)
			}
		}
	}

	// 链接识别
	if responseText == "" && (msgType == "private" || b.isGroupBVEnabled(groupID)) {
		// 先识别正文中的 BV 号
		if bv := b.reBV.FindString(cleanMsg); bv != "" {
			responseImg, responseText = b.parseBilibiliBV(bv)
		} else if strings.Contains(cleanMsg, "b23.tv") {
			// 再尝试识别 B 站短链，例如 https://b23.tv/xxxxxx
			shortURL := ""
			if m := b.reBilibiliShort.FindString(cleanMsg); m != "" {
				shortURL = m
			} else {
				// 兜底: 粗略截取第一个出现的 b23.tv 片段
				if idx := strings.Index(cleanMsg, "b23.tv/"); idx != -1 {
					end := idx
					for end < len(cleanMsg) && !unicode.IsSpace(rune(cleanMsg[end])) {
						end++
					}
					shortURL = cleanMsg[idx:end]
					if !strings.HasPrefix(shortURL, "http") {
						shortURL = "https://" + shortURL
					}
				}
			}
			if shortURL != "" {
				if bv, err := b.resolveBilibiliBVFromURL(shortURL); err == nil && bv != "" {
					responseImg, responseText = b.parseBilibiliBV(bv)
				}
			}
		}
	}
	if responseText == "" {
		for _, reYT := range b.reYouTube {
			if m := reYT.FindStringSubmatch(cleanMsg); len(m) >= 2 {
				responseImg, responseText = b.parseYouTubeVideo(m[1])
				break
			}
		}
	}

	// AI 对话
	shouldChat := false
	prompt := ""
	tempModel := ""
	if responseText == "" {
		switch {
		case strings.HasPrefix(commandMsg, "/ct "):
			shouldChat = true
			prompt = strings.TrimSpace(commandMsg[4:])
		case strings.HasPrefix(commandMsg, "/grok "):
			shouldChat = true
			tempModel = "grok"
			prompt = strings.TrimSpace(commandMsg[6:])
		case strings.HasPrefix(commandMsg, "/deepseek "):
			shouldChat = true
			tempModel = "deepseek"
			prompt = strings.TrimSpace(commandMsg[len("/deepseek "):])
		case msgType == "private":
			shouldChat = true
			prompt = cleanMsg
		case isAtMe:
			shouldChat = true
			// 简单去掉 @xxx
			prompt = strings.TrimSpace(regexp.MustCompile(`@\d+\s*`).ReplaceAllString(cleanMsg, ""))
		}
	}

	imagePaths := []string(nil)
	cleanups := []func(){}
	if shouldChat {
		// 1) 当前消息内的图片（CQ:image / CQ:mface）
		if hasImage {
			if paths, cleanup := b.saveIncomingImages(rawMsg); len(paths) > 0 {
				imagePaths = append(imagePaths, paths...)
				cleanups = append(cleanups, cleanup)
			}
		}

		// 2) 群聊：当“@机器人 + 引用(reply)”时，尝试从被引用消息中提取图片。
		// 说明：OneBot 的 reply 段只带 message_id，本条消息 raw_message 不一定包含图片段。
		if msgType == "group" && isAtMe && hasReply && replyMsgID > 0 {
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			data, err := client.Call(ctx, "get_msg", map[string]any{"message_id": replyMsgID})
			cancel()
			if err != nil {
				b.logger.Printf("get_msg 失败（message_id=%d）: %v", replyMsgID, err)
			} else {
				var d struct {
					Message any `json:"message"`
				}
				if err := json.Unmarshal(data, &d); err == nil {
					if paths, cleanup := b.saveIncomingImagesFromOneBotMessage(d.Message); len(paths) > 0 {
						imagePaths = append(imagePaths, paths...)
						cleanups = append(cleanups, cleanup)
					}
				}
			}
		}
	}
	if len(cleanups) > 0 {
		defer func() {
			for _, fn := range cleanups {
				fn()
			}
		}()
	}

	hasAnyImage := len(imagePaths) > 0
	if shouldChat && strings.TrimSpace(prompt) == "" && hasAnyImage {
		prompt = "请描述图片内容，并结合上下文回答我的问题。"
	}

	if shouldChat && strings.TrimSpace(prompt) != "" {
		groupIDPtr := (*string)(nil)
		if groupID != "" {
			groupIDPtr = &groupID
		}
		modelKey := tempModel
		if modelKey == "" {
			modelKey = b.getUserModel(userID, groupIDPtr)
		}
		// 图片输入目前仅对 GPT(Codex CLI) 做了适配；避免 Claude/Grok 丢失图片信息。
		if hasAnyImage {
			modelKey = "gpt"
		}
		systemPrompt := b.modelPrompts[modelKey]
		if systemPrompt == "" {
			systemPrompt = b.modelPrompts["gpt"]
		}
		// 组装历史
		b.memoryMu.RLock()
		history := []historyItem{}
		if up, ok := b.users[userID]; ok {
			history = append(history, up.History...)
		}
		b.memoryMu.RUnlock()
		msgs := []chatMessage{{Role: "system", Content: systemPrompt}}
		if events := b.recentUserEvents(userID, 12); len(events) > 0 {
			var evb strings.Builder
			evb.WriteString("用户近期事件记忆:\n")
			for _, ev := range events {
				evb.WriteString(fmt.Sprintf("- %s [%s] %s\n", ev.Time, ev.Type, ev.Detail))
			}
			msgs = append(msgs, chatMessage{Role: "system", Content: evb.String()})
		}
		for _, h := range history {
			if h.Content == "" {
				continue
			}
			msgs = append(msgs, chatMessage{Role: h.Role, Content: h.Content})
		}
		msgs = append(msgs, chatMessage{Role: "user", Content: prompt})
		msgs = compactChatMessages(msgs, maxContextChars)
		var ans string
		switch modelKey {
		case "claude":
			ans = b.callClaudeAPI(systemPrompt, msgs)
		case "grok":
			ans = b.callGrokAPI(msgs)
		case "deepseek":
			ans = b.callDeepSeekAPI(msgs)
		default:
			ans = b.callGPTAPI(msgs, imagePaths)
		}
		// 记忆中不保存绝对路径，避免泄露本机路径细节。
		memPrompt := prompt
		if hasAnyImage {
			if len(imagePaths) > 0 {
				memPrompt = memPrompt + fmt.Sprintf("\n[用户发送了 %d 张图片]", len(imagePaths))
			} else {
				memPrompt = memPrompt + "\n[用户发送了图片]"
			}
		}
		b.appendHistory(userID, "user", memPrompt)
		b.appendHistory(userID, "assistant", ans)
		responseText = fmt.Sprintf("🤖 [%s]\n%s", modelKey, ans)
	}

	if responseText != "" || responseImg != "" {
		if !shouldChat && strings.HasPrefix(strings.TrimSpace(commandMsg), "/") {
			memResult := responseText
			if responseImg != "" {
				memResult = strings.TrimSpace(memResult + "\n[机器人发送了一张图片]")
			}
			b.rememberBotAction(userID, groupID, commandMsg, memResult)
		}
		finalMsg := responseText
		if responseImg != "" && responseText != "" {
			finalMsg = fmt.Sprintf("[CQ:image,file=%s]\n%s", responseImg, responseText)
		} else if responseImg != "" {
			finalMsg = fmt.Sprintf("[CQ:image,file=%s]", responseImg)
		}
		target := msg.GroupID
		if msgType == "private" {
			target = msg.UserID
		}
		b.sendLongText(client, msgType, target, finalMsg, msg.SelfID)
	}
}

// ============ WebSocket 服务 ============

var upgrader = websocket.Upgrader{
	ReadBufferSize:  8192,
	WriteBufferSize: 8192,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func (b *qqBotServer) clientHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		b.logger.Printf("WebSocket 握手失败: %v", err)
		return
	}
	client := &wsClient{conn: conn, pending: make(map[string]chan []byte)}
	b.registerClient(client)
	b.logger.Println("New Client Connected")
	defer func() {
		b.logger.Println("Client Disconnected")
		b.unregisterClient(client)
		_ = conn.Close()
	}()
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				b.logger.Printf("读消息异常: %v", err)
			}
			return
		}
		// 兼容处理：同一条 WS 连接上既会收到事件，也会收到 action 的响应（带 echo）。
		var hint struct {
			Echo     string `json:"echo"`
			PostType string `json:"post_type"`
		}
		if err := json.Unmarshal(data, &hint); err == nil && hint.Echo != "" && hint.PostType == "" {
			if client.fulfillEcho(hint.Echo, data) {
				continue
			}
		}

		// 每条事件消息单独 goroutine 处理，避免阻塞读取
		payload := make([]byte, len(data))
		copy(payload, data)
		go b.processSingleMessage(client, payload)
	}
}

func (b *qqBotServer) run() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/", b.clientHandler)
	server := &http.Server{
		Addr:    wsListenAddr,
		Handler: mux,
	}

	// 监听关闭信号
	idleConnsClosed := make(chan struct{})
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		b.logger.Println("Shutting down...")
		close(b.shutdown)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
		close(idleConnsClosed)
	}()

	b.logger.Printf("🚀 Server Started on %s", wsListenAddr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	<-idleConnsClosed
	b.wg.Wait()
	return nil
}

func main() {
	// 先从配置文件加载 API Key 等配置（如存在则覆盖环境变量）
	loadConfigFromFile(configFilePath)

	bot := newQQBotServer()
	if err := bot.run(); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
