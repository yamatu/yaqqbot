package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
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
	"syscall"
	"time"
	"unicode"

	"github.com/gorilla/websocket"

	"qq_client/internal/codexcli"
	"qq_client/internal/openaiutil"
)

// QQBot 的 Go 实现。
// 当前版本在保持核心能力的基础上，补充了配置与密钥的工程化管理。

// ============ 配置区域 ============

const (
	// 持久化文件
	memoryFile         = "user_memory.json"
	groupSettingsKey   = "__group_settings__"
	nginxServersFile   = "nginx_servers.json"
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

	amapAPIKey      = os.Getenv("AMAP_API_KEY")
	bilibiliAPIBase = envOrDefault("BILIBILI_API_BASE", "https://api.bilibili.com/x/web-interface/view")

	// SOCKS5/HTTP 代理（例如 socks5://127.0.0.1:41457 或 http://127.0.0.1:1080）
	socks5Proxy = os.Getenv("SOCKS5_PROXY")
)

// 提示词文件
var promptFiles = map[string]string{
	"grok":   "grok.txt",
	"gpt":    "gpt.txt",
	"claude": "claude.txt",
}

// ============ 数据结构 ============

type historyItem struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type userProfile struct {
	History []historyItem `json:"history"`
	Model   string        `json:"model"`
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
	UserID      int64  `json:"user_id"`
	GroupID     int64  `json:"group_id"`
	RawMessage  string `json:"raw_message"`
	SelfID      int64  `json:"self_id"`
}

// WebSocket 客户端封装，保证写操作串行。
type wsClient struct {
	conn    *websocket.Conn
	writeMu sync.Mutex
}

func (c *wsClient) WriteJSON(v any) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.conn.WriteJSON(v)
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

	// HTTP 客户端
	httpClient  *http.Client
	proxyClient *http.Client

	// 正则
	reCQ            *regexp.Regexp
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

// fileConfig 对应 configs/config.json，用来从文件加载 API Key 等敏感配置。
type fileConfig struct {
	ClaudeAPIKey    string `json:"claude_api_key"`
	ClaudeAPIBase   string `json:"claude_api_base"`
	ClaudeModel     string `json:"claude_model"`
	GPTAPIKey       string `json:"gpt_api_key"`
	GPTAPIBase      string `json:"gpt_api_base"`
	GPTModel        string `json:"gpt_model"`
	GrokAPIKey      string `json:"grok_api_key"`
	GrokAPIBase     string `json:"grok_api_base"`
	GrokModel       string `json:"grok_model"`
	AMapAPIKey      string `json:"amap_api_key"`
	BilibiliAPIBase string `json:"bilibili_api_base"`
	Socks5Proxy     string `json:"socks5_proxy"`
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
	if err := json.Unmarshal(data, &cfg); err != nil {
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
		httpClient:       newHTTPClient(),
		proxyClient:      newProxyHTTPClient(),
		reCQ:             reCQ,
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
	bot.startBackgroundSave()

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
	if model != "claude" && model != "gpt" && model != "grok" {
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
	if model != "gpt" && model != "claude" && model != "grok" {
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
		"grok":   "你是Grok，语气轻松幽默。",
		"gpt":    "你是AI助手，请简洁回答。",
		"claude": "你是Claude，请详细且逻辑清晰地回答。",
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
	req.Header.Set("User-Agent", "Mozilla/5.0")

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
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
	resp, err := b.httpClient.Do(req)
	if err != nil {
		return "", fmt.Sprintf("❌ 解析错误: %v", err)
	}
	defer resp.Body.Close()
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
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return "", fmt.Sprintf("❌ 解析错误: %v", err)
	}
	if raw.Code != 0 {
		return "", fmt.Sprintf("❌ 视频失效: %s", raw.Message)
	}
	info := raw.Data
	res := fmt.Sprintf("📺 %s\n👤 UP: %s\n📊 播放: %d  👍 %d  💰 %d\n🔗 https://www.bilibili.com/video/%s",
		info.Title, info.Owner.Name, info.Stat.View, info.Stat.Like, info.Stat.Coin, bvid)
	return info.Pic, res
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

// ============ AI 调用 ============

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

func (b *qqBotServer) callGPTAPI(messages []chatMessage) string {
	if gptAPIKey == "" {
		return "❌ GPT 未配置 API Key"
	}
	apiBase := openaiutil.NormalizeAPIBase(gptAPIBase)
	origin := openaiutil.OriginFromAPIBase(apiBase)

	// 特殊处理：codex-api.packycode.com 明确限制“仅允许官方 Codex CLI 访问”。
	// 这里不尝试伪装/绕过，而是直接调用本机 Codex CLI 来完成对话（需要你已安装并登录/配置）。
	if strings.Contains(strings.ToLower(apiBase), "codex-api.packycode.com") {
		prompt := buildConversationPrompt(messages)
		out, err := codexcli.Exec(context.Background(), prompt, codexcli.ExecOptions{
			Bin:     envOrDefault("CODEX_CLI_BIN", "codex"),
			Model:   gptModel,
			APIBase: apiBase,
			APIKey:  gptAPIKey,
			EnvKey:  "GPT_API_KEY",
			WireAPI: envOrDefault("CODEX_WIRE_API", "responses"),
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
func buildConversationPrompt(messages []chatMessage) string {
	var b strings.Builder
	b.WriteString("你是聊天助手。请不要执行任何命令、不要读写文件、不要修改仓库，只需要回答用户的最后一个问题。\n\n")
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

// ============ 消息处理 ============

func (b *qqBotServer) stripCQCodes(message string) string {
	if message == "" {
		return ""
	}
	return strings.TrimSpace(b.reCQ.ReplaceAllString(message, ""))
}

func (b *qqBotServer) sendLongText(client *wsClient, messageType string, targetID int64, text string) {
	if text == "" {
		return
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

func (b *qqBotServer) processSingleMessage(client *wsClient, payload []byte) {
	var msg cqMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		return
	}
	if msg.PostType != "message" {
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
	isAtMe := msgType == "group" && strings.Contains(rawMsg, "[CQ:at,qq="+selfID+"]")

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
		case strings.HasPrefix(cleanMsg, "/help"):
			responseText = "🤖 帮助:\n" +
				"/ct [问题]           - 问答\n" +
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
		case strings.HasPrefix(cleanMsg, "/ping "):
			responseText = b.pingViaProxy(strings.TrimSpace(cleanMsg[6:]))
		case strings.HasPrefix(cleanMsg, "/nginx"):
			args := strings.TrimSpace(cleanMsg[len("/nginx"):])
			responseText = b.handleNginxCommand(args, userID, msgType)
		case strings.HasPrefix(cleanMsg, "/天气 "):
			responseText = b.getWeather(strings.TrimSpace(cleanMsg[4:]))
		case strings.HasPrefix(cleanMsg, "/rs"):
			responseText = b.getBilibiliHotSearch()
		case strings.HasPrefix(cleanMsg, "/epic"):
			responseText = b.getEpicFreeGames()
		case strings.HasPrefix(cleanMsg, "/set "):
			model := strings.ToLower(strings.TrimSpace(cleanMsg[5:]))
			if b.setUserModel(userID, model) {
				responseText = "✅ 个人模型: " + model
			} else {
				responseText = "❌ 未知模型"
			}
		case strings.HasPrefix(cleanMsg, "/setall ") && msgType == "group":
			model := strings.ToLower(strings.TrimSpace(cleanMsg[8:]))
			if b.setGroupDefaultModel(groupID, model) {
				responseText = "✅ 群默认模型: " + model
			} else {
				responseText = "❌ 未知模型"
			}
		case strings.TrimSpace(cleanMsg) == "/clear":
			if b.clearUserMemory(userID) {
				responseText = "🧹 记忆已清除"
			} else {
				responseText = "ℹ️ 无记忆可清除"
			}
		case strings.HasPrefix(cleanMsg, "/bv "):
			arg := strings.TrimSpace(cleanMsg[4:])
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
		case strings.HasPrefix(cleanMsg, "/ct "):
			shouldChat = true
			prompt = strings.TrimSpace(cleanMsg[4:])
		case strings.HasPrefix(cleanMsg, "/grok "):
			shouldChat = true
			tempModel = "grok"
			prompt = strings.TrimSpace(cleanMsg[6:])
		case msgType == "private":
			shouldChat = true
			prompt = cleanMsg
		case isAtMe:
			shouldChat = true
			// 简单去掉 @xxx
			prompt = strings.TrimSpace(regexp.MustCompile(`@\d+\s*`).ReplaceAllString(cleanMsg, ""))
		}
	}
	if shouldChat && prompt != "" {
		groupIDPtr := (*string)(nil)
		if groupID != "" {
			groupIDPtr = &groupID
		}
		modelKey := tempModel
		if modelKey == "" {
			modelKey = b.getUserModel(userID, groupIDPtr)
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
		for _, h := range history {
			if h.Content == "" {
				continue
			}
			msgs = append(msgs, chatMessage{Role: h.Role, Content: h.Content})
		}
		msgs = append(msgs, chatMessage{Role: "user", Content: prompt})
		var ans string
		switch modelKey {
		case "claude":
			ans = b.callClaudeAPI(systemPrompt, msgs)
		case "grok":
			ans = b.callGrokAPI(msgs)
		default:
			ans = b.callGPTAPI(msgs)
		}
		b.appendHistory(userID, "user", prompt)
		b.appendHistory(userID, "assistant", ans)
		responseText = fmt.Sprintf("🤖 [%s]\n%s", modelKey, ans)
	}

	if responseText != "" {
		finalMsg := responseText
		if responseImg != "" {
			finalMsg = fmt.Sprintf("[CQ:image,file=%s]\n%s", responseImg, responseText)
		}
		target := msg.GroupID
		if msgType == "private" {
			target = msg.UserID
		}
		b.sendLongText(client, msgType, target, finalMsg)
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
	client := &wsClient{conn: conn}
	b.logger.Println("New Client Connected")
	defer func() {
		b.logger.Println("Client Disconnected")
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
		// 每条消息单独 goroutine 处理，避免阻塞读取
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
