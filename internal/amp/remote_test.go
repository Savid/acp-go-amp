package amp

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/savid/acp-go-core/process"
	"github.com/stretchr/testify/require"
)

func TestMissingRequiresAnAuthenticatedMissingVerdict(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name           string
		status         int
		body           string
		missing, fails bool
	}{
		{name: "missing", status: 200, body: `{"ok":false,"error":{"code":"thread-not-found"}}`, missing: true},
		{name: "present", status: 200, body: `{"ok":true}`},
		{name: "unauthorized", status: 401, body: `{"ok":false,"error":{"code":"thread-not-found"}}`, fails: true},
		{name: "forbidden", status: 403, body: `{}`, fails: true},
		{name: "unavailable", status: 503, body: `{}`, fails: true},
		{name: "unknown-route", status: 404, body: `{}`, fails: true},
		{name: "denied", status: 200, body: `{"ok":false,"error":{"code":"permission-denied"}}`, fails: true},
		{name: "malformed", status: 200, body: `{}`, fails: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer test-native-key" {
					w.WriteHeader(http.StatusUnauthorized)

					return
				}
				w.WriteHeader(tc.status)
				_, _ = fmt.Fprint(w, tc.body)
			}))
			defer server.Close()
			remote, err := NewRemote(process.Request{Env: []string{"AMP_API_KEY=test-native-key"}}, server.URL+"/")
			require.NoError(t, err)
			defer remote.Close()
			missing, err := remote.Missing(t.Context(), "T-00000000-0000-0000-0000-000000000001")
			require.Equal(t, tc.missing, missing)
			if tc.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRemoteReadsCurrentNativeCredentials(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(root, "amp"), 0700))
	path := filepath.Join(root, "amp", "secrets.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"apiKey@https://ampcode.com/":"first"}`), 0600))
	remote, err := NewRemote(process.Request{Dir: root, Env: []string{"XDG_DATA_HOME=" + root}}, "https://ampcode.com/")
	require.NoError(t, err)
	defer remote.Close()
	key, err := remote.apiKey()
	require.NoError(t, err)
	require.Equal(t, "first", key)
	require.NoError(t, os.WriteFile(path, []byte(`{"apiKey@https://ampcode.com/":"refreshed"}`), 0600))
	key, err = remote.apiKey()
	require.NoError(t, err)
	require.Equal(t, "refreshed", key)
}
