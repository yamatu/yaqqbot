package main

import (
	"strings"
	"testing"
)

func TestCompactChatMessagesPreservesLeadingSystemSearchContext(t *testing.T) {
	messages := []chatMessage{
		{Role: "system", Content: "base prompt"},
		{Role: "system", Content: "联网搜索摘要: current facts"},
	}
	for i := 0; i < 30; i++ {
		messages = append(messages, chatMessage{Role: "user", Content: strings.Repeat("history ", 40)})
		messages = append(messages, chatMessage{Role: "assistant", Content: strings.Repeat("answer ", 40)})
	}

	compact := compactChatMessages(messages, 1200)
	if len(compact) < 2 {
		t.Fatalf("compact messages too short: %d", len(compact))
	}
	if compact[0].Content != "base prompt" {
		t.Fatalf("base system prompt was not preserved: %#v", compact[0])
	}
	if !strings.Contains(compact[1].Content, "联网搜索摘要") {
		t.Fatalf("search context system message was not preserved: %#v", compact[1])
	}
}

func TestBuildDeepSeekPromptWithSearchIncludesSearchAndQuestion(t *testing.T) {
	prompt := buildDeepSeekPromptWithSearch("谷歌公司创始时间", "联网搜索摘要: Google founded in 1998-09-04")
	if !strings.Contains(prompt, "【联网搜索结果】") {
		t.Fatalf("missing search section: %s", prompt)
	}
	if !strings.Contains(prompt, "Google founded in 1998-09-04") {
		t.Fatalf("missing search content: %s", prompt)
	}
	if !strings.Contains(prompt, "谷歌公司创始时间") {
		t.Fatalf("missing user question: %s", prompt)
	}
}

func TestBuildDetailedSearchPromptRequestsSpecificDetails(t *testing.T) {
	prompt := buildDetailedSearchPrompt("NiKo夺冠", false, true)
	for _, want := range []string{"详细事实", "电竞", "赛事名称", "比分", "来源"} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("search prompt missing %q: %s", want, prompt)
		}
	}
	if strings.Contains(prompt, "简短摘要") {
		t.Fatalf("search prompt should not ask for short summary: %s", prompt)
	}
}

func TestLowDetailSearchReasonRejectsGenericProjectSummary(t *testing.T) {
	answer := "在2026年7月，GitHub上涌现了许多好玩且功能强大的开源项目。上述项目主要体现了向自托管、代理自动化和多代理协作演进的趋势。"
	if reason := lowDetailSearchReason("github 好玩的开源项目", answer); reason == "" {
		t.Fatalf("expected generic project summary to be rejected")
	}
}

func TestLowDetailSearchReasonAcceptsRepoEvidence(t *testing.T) {
	answer := "1. OpenAI/codex - https://github.com/openai/codex - 终端编码代理。\n2. astral-sh/uv - https://github.com/astral-sh/uv - Python 包管理工具。"
	if reason := lowDetailSearchReason("github 好玩的开源项目", answer); reason != "" {
		t.Fatalf("expected repo evidence to pass, got: %s", reason)
	}
}
