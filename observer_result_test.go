package ampacp

import (
	"errors"
	"testing"

	"github.com/coder/acp-go-sdk"
)

func TestPromptResultForObserver(t *testing.T) {
	if got := promptResultForObserver(acp.PromptResponse{StopReason: acp.StopReasonCancelled}, errors.New("boom"), "amp-model"); got.Err == nil || got.Model != "amp-model" || got.StopReason != string(acp.StopReasonCancelled) {
		t.Fatalf("no-usage result = %#v", got)
	}

	resp := acp.PromptResponse{
		StopReason: acp.StopReasonEndTurn,
		Usage: &acp.Usage{
			InputTokens:       11,
			OutputTokens:      22,
			TotalTokens:       33,
			CachedReadTokens:  acp.Ptr(4),
			CachedWriteTokens: acp.Ptr(5),
			ThoughtTokens:     acp.Ptr(6),
		},
	}
	got := promptResultForObserver(resp, nil, "")
	if got.InputTokens != 11 || got.OutputTokens != 22 || got.TotalTokens != 33 {
		t.Fatalf("token totals = %#v", got)
	}
	if got.CachedReadTokens != 4 || got.CachedWriteTokens != 5 || got.ThoughtTokens != 6 {
		t.Fatalf("cache/thought tokens = %#v", got)
	}
}
