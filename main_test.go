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
