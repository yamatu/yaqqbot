package codexcli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"
	"unicode/utf8"
)

type ExecOptions struct {
	// Codex CLI 可执行文件名/路径，默认 "codex"
	Bin string

	// 目标模型名（会传给 `codex exec --model`）
	Model string

	// 目标 API Base（例如 https://codex-api.packycode.com/v1）
	APIBase string

	// 目标 API Key（会注入到 EnvKey 指定的环境变量中）
	APIKey string

	// 传给 Codex CLI 的 env_key（例如 "GPT_API_KEY"）
	EnvKey string

	// wire_api: "responses" 或 "chat"
	WireAPI string

	// SkipGitRepoCheck 为 true 时，会给 `codex exec` 追加 `--skip-git-repo-check`，
	// 用于在“非可信目录 / 非 Git 仓库”（例如 Windows 的 system32 或临时目录）中运行。
	SkipGitRepoCheck *bool

	// 运行目录：为了避免 Codex 读取当前仓库上下文，默认切到系统临时目录
	WorkDir string

	// 输出超时保护
	Timeout time.Duration
}

func (o ExecOptions) withDefaults() ExecOptions {
	out := o
	if strings.TrimSpace(out.Bin) == "" {
		out.Bin = "codex"
	}
	if strings.TrimSpace(out.EnvKey) == "" {
		out.EnvKey = "GPT_API_KEY"
	}
	if strings.TrimSpace(out.WireAPI) == "" {
		out.WireAPI = "responses"
	}
	if strings.TrimSpace(out.WorkDir) == "" {
		out.WorkDir = os.TempDir()
	}
	if out.Timeout <= 0 {
		out.Timeout = 180 * time.Second
	}
	// 默认开启：我们通常在临时目录执行，不属于 Codex 的“可信目录”校验范围。
	if out.SkipGitRepoCheck == nil {
		v := true
		out.SkipGitRepoCheck = &v
	}
	return out
}

type execCaps struct {
	hasColor             bool
	hasSandbox           bool
	hasSkipGitRepoCheck  bool
	hasModel             bool
	hasOutputLastMessage bool
	hasConfig            bool
}

type rootCaps struct {
	hasConfig         bool
	hasModel          bool
	hasSandbox        bool
	hasAskForApproval bool
	hasCd             bool
}

func readHelpText(ctx context.Context, bin string, args ...string) string {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, args...)
	out, _ := cmd.CombinedOutput()
	return string(out)
}

func detectCaps(ctx context.Context, bin string) (rootCaps, execCaps) {
	helpExec := readHelpText(ctx, bin, "exec", "--help")
	helpRoot := readHelpText(ctx, bin, "--help")

	rc := rootCaps{
		hasConfig:         strings.Contains(helpRoot, "--config"),
		hasModel:          strings.Contains(helpRoot, "--model"),
		hasSandbox:        strings.Contains(helpRoot, "--sandbox"),
		hasAskForApproval: strings.Contains(helpRoot, "--ask-for-approval"),
		hasCd:             strings.Contains(helpRoot, "--cd"),
	}
	ec := execCaps{
		hasColor:             strings.Contains(helpExec, "--color"),
		hasSandbox:           strings.Contains(helpExec, "--sandbox"),
		hasSkipGitRepoCheck:  strings.Contains(helpExec, "--skip-git-repo-check") || strings.Contains(helpRoot, "--skip-git-repo-check"),
		hasModel:             strings.Contains(helpExec, "--model"),
		hasOutputLastMessage: strings.Contains(helpExec, "--output-last-message"),
		// 兼容：有些版本把 --config 作为全局参数；但也可能在 exec 中标记为 global
		hasConfig: strings.Contains(helpExec, "--config") || strings.Contains(helpRoot, "--config"),
	}
	return rc, ec
}

func decodeMaybeWindows(bytesIn []byte) string {
	if len(bytesIn) == 0 {
		return ""
	}
	if utf8.Valid(bytesIn) {
		return strings.TrimSpace(string(bytesIn))
	}
	// 没有额外依赖时无法可靠自动解码 Windows 代码页（CP936/GBK 等），这里保留原始字节。
	return strings.TrimSpace(string(bytesIn))
}

func runCodex(ctx context.Context, bin string, args []string, env []string, workDir string, stdin string) ([]byte, []byte, error) {
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Dir = workDir
	cmd.Env = env
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return stdout.Bytes(), stderr.Bytes(), err
}

// Exec 通过官方 Codex CLI 执行一次“纯对话”请求，返回最后一条助手消息。
// 说明：
// - codex-api.packycode.com 等入口会校验“是否官方 Codex CLI”，普通 HTTP 客户端会被拒绝；
// - 这里不尝试绕过限制，而是直接调用官方 Codex CLI 来完成请求。
func Exec(ctx context.Context, prompt string, opts ExecOptions) (string, error) {
	opts = opts.withDefaults()
	if strings.TrimSpace(opts.Model) == "" {
		return "", errors.New("model 不能为空")
	}
	if strings.TrimSpace(opts.APIBase) == "" {
		return "", errors.New("api_base 不能为空")
	}
	if strings.TrimSpace(opts.APIKey) == "" {
		return "", errors.New("api_key 不能为空")
	}

	ctx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()

	rootCaps, execCaps := detectCaps(ctx, opts.Bin)

	// 只有支持 --output-last-message 时才创建临时文件
	lastMsgPath := ""
	if execCaps.hasOutputLastMessage {
		tmpDir := os.TempDir()
		lastMsgFile, err := os.CreateTemp(tmpDir, "codex-last-message-*.txt")
		if err != nil {
			return "", fmt.Errorf("创建临时文件失败: %w", err)
		}
		lastMsgPath = lastMsgFile.Name()
		_ = lastMsgFile.Close()
		defer func() {
			_ = os.Remove(lastMsgPath)
		}()
	}

	tmpDir := os.TempDir()
	_ = tmpDir

	// 使用 config override 注入自定义 provider（避免要求用户手改 ~/.codex/config.toml）
	// provider 名称避免特殊字符，减少 TOML/override 解析风险
	const providerName = "packy_codex"

	// 说明：你提供的 `codex -h` 显示 `--ask-for-approval/--sandbox/--config/--cd/--model` 都是全局参数，
	// 需要放在子命令 `exec` 之前，否则某些版本会提示 `unexpected argument`。
	globalArgs := []string{}

	if rootCaps.hasAskForApproval {
		globalArgs = append(globalArgs, "--ask-for-approval", "never")
	}
	if rootCaps.hasSandbox {
		globalArgs = append(globalArgs, "--sandbox", "read-only")
	}
	if rootCaps.hasCd && strings.TrimSpace(opts.WorkDir) != "" {
		globalArgs = append(globalArgs, "--cd", opts.WorkDir)
	}
	if rootCaps.hasModel {
		globalArgs = append(globalArgs, "--model", opts.Model)
	}

	// 中转 provider 配置：优先按全局 --config 注入；如果全局不支持，则尝试在 exec 参数中注入（部分版本标记为 global）。
	configArgs := []string{
		"--config", "model_provider=" + providerName,
		// 一些 codex 版本要求 provider 定义中必须包含 name 字段
		"--config", "model_providers." + providerName + ".name=" + providerName,
		"--config", "model_providers." + providerName + ".base_url=" + opts.APIBase,
		"--config", "model_providers." + providerName + ".env_key=" + opts.EnvKey,
		"--config", "model_providers." + providerName + ".wire_api=" + opts.WireAPI,
	}
	if rootCaps.hasConfig {
		globalArgs = append(globalArgs, configArgs...)
	} else if execCaps.hasConfig {
		// 先留空，后面拼到 execArgs
	}

	execArgs := []string{"exec"}

	if execCaps.hasColor {
		execArgs = append(execArgs, "--color", "never")
	}
	// sandbox 优先使用全局；全局不支持时才放到 exec
	if !rootCaps.hasSandbox && execCaps.hasSandbox {
		execArgs = append(execArgs, "--sandbox", "read-only")
	}
	// model 优先使用全局；全局不支持时才放到 exec
	if !rootCaps.hasModel && execCaps.hasModel {
		execArgs = append(execArgs, "--model", opts.Model)
	}

	if opts.SkipGitRepoCheck != nil && *opts.SkipGitRepoCheck && execCaps.hasSkipGitRepoCheck {
		execArgs = append(execArgs, "--skip-git-repo-check")
	}
	if execCaps.hasOutputLastMessage && lastMsgPath != "" {
		execArgs = append(execArgs, "--output-last-message", lastMsgPath)
	}
	if !rootCaps.hasConfig && execCaps.hasConfig {
		execArgs = append(execArgs, configArgs...)
	}

	prompt = strings.TrimSpace(prompt)
	if prompt == "" {
		return "", errors.New("prompt 不能为空")
	}

	// 注入 API Key 到 env_key 对应变量，同时保留现有环境变量
	env := os.Environ()
	env = append(env, opts.EnvKey+"="+opts.APIKey)
	// 尽可能兼容不同实现的环境变量名称（不同 codex 版本/发行版可能读取不同 key）
	env = append(env, "OPENAI_API_KEY="+opts.APIKey)
	env = append(env, "OPENAI_BASE_URL="+opts.APIBase)
	env = append(env, "OPENAI_API_BASE="+opts.APIBase)

	// Windows 下把长 prompt 放到命令行参数里很容易触发长度限制，导致“参数/输入太长”等报错且出现乱码。
	// 因此优先尝试 stdin 模式：不带 [PROMPT] 位置参数，直接把 prompt 写入 stdin。
	tryStdinFirst := runtime.GOOS == "windows" || len(prompt) > 8000

	var stdoutBytes, stderrBytes []byte
	var runErr error

	if tryStdinFirst {
		// 有些 codex 版本不从 stdin 读取 prompt，会进入交互等待；这里给 stdin 尝试一个较短超时，避免机器人卡死。
		stdinCtx, stdinCancel := context.WithTimeout(ctx, 25*time.Second)
		stdoutBytes, stderrBytes, runErr = runCodex(stdinCtx, opts.Bin, append(append([]string{}, globalArgs...), execArgs...), env, opts.WorkDir, prompt)
		stdinCancel()

		// 若 stdin 模式失败（常见为旧版要求 [PROMPT]），回退到短位置参数模式
		if runErr != nil {
			stderrLower := strings.ToLower(string(stderrBytes))
			// 超时也视为“可能不支持 stdin”，直接回退
			if errors.Is(runErr, context.DeadlineExceeded) || errors.Is(stdinCtx.Err(), context.DeadlineExceeded) ||
				strings.Contains(stderrLower, "usage:") || strings.Contains(stderrLower, "prompt") {
				// 兼容一些实现：使用 "-" 作为短 prompt 参数，并把真实 prompt 放在 stdin
				stdoutBytes, stderrBytes, runErr = runCodex(ctx, opts.Bin, append(append(append([]string{}, globalArgs...), execArgs...), "-"), env, opts.WorkDir, prompt)
				if runErr != nil {
					// 最后回退：截断 prompt 进位置参数，尽量给出可读错误
					shortPrompt := prompt
					if len(shortPrompt) > 2000 {
						shortPrompt = shortPrompt[:2000]
					}
					stdoutBytes, stderrBytes, runErr = runCodex(ctx, opts.Bin, append(append(append([]string{}, globalArgs...), execArgs...), shortPrompt), env, opts.WorkDir, "")
				}
			}
		}
	} else {
		stdoutBytes, stderrBytes, runErr = runCodex(ctx, opts.Bin, append(append(append([]string{}, globalArgs...), execArgs...), prompt), env, opts.WorkDir, "")
	}

	if runErr != nil {
		// 兼容“未安装 codex”场景
		var execErr *exec.Error
		if errors.As(runErr, &execErr) && errors.Is(execErr.Err, exec.ErrNotFound) {
			return "", fmt.Errorf("未找到 Codex CLI（%s），请先安装并登录: %w", opts.Bin, runErr)
		}
		// 超时
		if errors.Is(runErr, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return "", fmt.Errorf("Codex CLI 调用超时（%s）", opts.Timeout)
		}
		combinedBytes := bytes.TrimSpace(stderrBytes)
		if len(combinedBytes) == 0 {
			combinedBytes = bytes.TrimSpace(stdoutBytes)
		}
		if len(combinedBytes) == 0 {
			combinedBytes = []byte(runErr.Error())
		}
		return "", fmt.Errorf("Codex CLI 调用失败: %s", decodeMaybeWindows(combinedBytes))
	}

	out := ""
	if lastMsgPath != "" {
		if data, err := os.ReadFile(lastMsgPath); err == nil {
			out = strings.TrimSpace(string(data))
		}
	}
	if out == "" {
		// 旧版本/不支持 --output-last-message：直接取 stdout
		out = decodeMaybeWindows(stdoutBytes)
	}
	if out == "" {
		return "", errors.New("Codex 返回为空")
	}

	// Windows 下可能会出现 CRLF；统一处理
	out = strings.ReplaceAll(out, "\r\n", "\n")

	out = strings.TrimSpace(out)
	return out, nil
}
