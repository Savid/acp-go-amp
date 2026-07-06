package amp

import "testing"

func TestHasAPIKey(t *testing.T) {
	t.Setenv("AMP_API_KEY", "")
	if HasAPIKey(nil) {
		t.Fatal("empty process env reported a key")
	}
	if HasAPIKey(map[string]string{"AMP_API_KEY": "  "}) {
		t.Fatal("whitespace override reported a key")
	}
	if !HasAPIKey(map[string]string{"AMP_API_KEY": "override"}) {
		t.Fatal("override key not detected")
	}

	t.Setenv("AMP_API_KEY", "process")
	if !HasAPIKey(nil) {
		t.Fatal("process env key not detected")
	}
	if HasAPIKey(map[string]string{"AMP_API_KEY": ""}) {
		t.Fatal("empty override did not win over process env")
	}
}
