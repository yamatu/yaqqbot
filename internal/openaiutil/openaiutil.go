package openaiutil

import (
	"encoding/json"
	"net/url"
	"strings"
)

// NormalizeAPIBase 负责把用户可能误填的 API Base 归一化为「.../v1」级别。
// 常见误区：把 Codex CLI 的前缀 https://api.openai.com/v1/codex 直接填进来，
// 进而导致拼接后的 URL 变成 /v1/codex/chat/completions，触发权限报错。
func NormalizeAPIBase(raw string) string {
	base := strings.TrimSpace(raw)
	base = strings.TrimRight(base, "/")

	// 仅处理「末尾」的 /codex，避免误伤包含同名路径的代理服务
	// 例：https://api.openai.com/v1/codex  -> https://api.openai.com/v1
	if strings.HasSuffix(base, "/codex") {
		base = strings.TrimSuffix(base, "/codex")
		base = strings.TrimRight(base, "/")
	}
	return base
}

func OriginFromAPIBase(apiBase string) string {
	u, err := url.Parse(apiBase)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return strings.TrimRight(apiBase, "/")
	}
	return u.Scheme + "://" + u.Host
}

// 尽量从 Responses API 的返回中提取文本。
// OpenAI 文档中推荐直接读取 response.output_text；部分兼容实现可能只返回 output 数组。
func ExtractResponsesOutputText(rawJSON []byte) string {
	var r struct {
		OutputText string `json:"output_text"`
		Output     []struct {
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"output"`
	}
	if err := json.Unmarshal(rawJSON, &r); err == nil {
		if strings.TrimSpace(r.OutputText) != "" {
			return strings.TrimSpace(r.OutputText)
		}
		var parts []string
		for _, o := range r.Output {
			for _, c := range o.Content {
				if c.Type == "output_text" && strings.TrimSpace(c.Text) != "" {
					parts = append(parts, strings.TrimSpace(c.Text))
				}
			}
		}
		if len(parts) > 0 {
			return strings.Join(parts, "\n")
		}
	}
	return ""
}
