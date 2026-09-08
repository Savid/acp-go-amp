package ampacp

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
)

func TestManagedHandoffRefusesPreparedDomains(t *testing.T) {
	for _, relation := range []string{"same", "ancestor", "descendant", "symlink"} {
		t.Run(relation, func(t *testing.T) {
			base := t.TempDir()
			scratch := filepath.Join(base, "scratch")
			require.NoError(t, os.Mkdir(scratch, 0o700))
			read := scratch
			switch relation {
			case "ancestor":
				read = base
			case "descendant":
				read = filepath.Join(scratch, "read")
				require.NoError(t, os.Mkdir(read, 0o700))
			case "symlink":
				read = filepath.Join(base, "alias")
				if err := os.Symlink(scratch, read); err != nil {
					if runtime.GOOS == "windows" {
						t.Skip("symlink privilege unavailable")
					}
					t.Fatal(err)
				}
			}
			authority := newRecordingAuthority()
			agent := newTestAgent(WithHostAuthority(authority), WithScratchDir(scratch), WithInputHandoffRoot(read))
			t.Cleanup(func() { require.NoError(t, agent.Close()) })
			_, err := newAgentSession(t.Context(), agent, "image", t.TempDir(), parsedSessionMeta{}, "", nil)
			require.NoError(t, err)
			require.NotEmpty(t, authority.events, "the opaque session exists before image mapping")
			// A valid declaration must be refused by the root boundary even when
			// its file is absent. The adapter cannot inspect the prepared domain.
			block := handoffBlock(filepath.Join(read, "unknown.png"), imageMIMEPNG, []byte("x"))
			_, err = promptInputWithPolicy(t.Context(), []acp.ContentBlock{block}, agent.promptImagePolicy())
			requireHandoffError(t, err, 0, imageErrorPathNotAllowed, handoffRootUnopenableMessage)
			block.Image.MimeType = "text/plain"
			_, err = promptInputWithPolicy(t.Context(), []acp.ContentBlock{block}, agent.promptImagePolicy())
			require.Contains(t, err.Error(), imageErrorInvalidMediaType, "declaration gates precede root access")
		})
	}
}

func TestManagedHandoffPinsDisjointDescriptorThroughSessionReclaim(t *testing.T) {
	base := t.TempDir()
	read := filepath.Join(base, "read")
	data, err := base64.StdEncoding.DecodeString(validPNGBase64)
	require.NoError(t, err)
	path := writeHandoffFile(t, read, "image.png", data)
	authority := newRecordingAuthority()
	agent := newTestAgent(WithHostAuthority(authority), WithScratchDir(filepath.Join(base, "scratch")), WithInputHandoffRoot(read))
	t.Cleanup(func() { require.NoError(t, agent.Close()) })
	authority.inspectPrepare = func(string) error {
		require.NotNil(t, agent.handoff.root, "read descriptor must precede preparation")

		return nil
	}
	session, err := newAgentSession(t.Context(), agent, "image", t.TempDir(), parsedSessionMeta{}, "", nil)
	require.NoError(t, err)
	root, refusal := agent.managedHandoffRoot()
	require.Nil(t, refusal)
	// Replacing the read pathname does not redirect the pinned descriptor.
	require.NoError(t, os.Rename(read, filepath.Join(base, "original")))
	writeHandoffFile(t, read, "image.png", []byte("replacement"))
	for range 2 {
		_, err = promptInputWithPolicy(t.Context(), []acp.ContentBlock{handoffBlock(path, imageMIMEPNG, data)}, agent.promptImagePolicy())
		require.NoError(t, err)
	}
	require.NoError(t, session.Close(t.Context()))
	_, err = promptInputWithPolicy(t.Context(), []acp.ContentBlock{handoffBlock(path, imageMIMEPNG, data)}, agent.promptImagePolicy())
	require.NoError(t, err, "session reclaim does not retire the Agent's read root")
	require.NoError(t, agent.Close())
	_, err = root.Stat(".")
	require.Error(t, err, "Agent.Close releases the retained descriptor")
}
