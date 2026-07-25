package ampacp

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
)

// handoffDigest renders the envelope digest for the given bytes.
func handoffDigest(data []byte) string {
	sum := sha256.Sum256(data)

	return hex.EncodeToString(sum[:])
}

// writeHandoffFile materializes bytes at a subpath of root, creating parents.
func writeHandoffFile(t *testing.T, root, subpath string, data []byte) string {
	t.Helper()

	path := filepath.Join(root, subpath)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
	require.NoError(t, os.WriteFile(path, data, 0o600))

	return path
}

func fileURI(path string) string {
	return "file://" + filepath.ToSlash(path)
}

// handoffBlock builds a handoff-form image content block: empty data, a file URI
// and the family handoff envelope.
func handoffBlock(path, mimeType string, data []byte) acp.ContentBlock {
	return handoffBlockWithEnvelope(path, mimeType, map[string]any{
		handoffFieldVersion:   handoffVersion,
		handoffFieldDigest:    handoffDigest(data),
		handoffFieldSizeBytes: len(data),
	})
}

func handoffBlockWithEnvelope(path, mimeType string, envelope any) acp.ContentBlock {
	block := acp.ImageBlock("", mimeType)
	block.Image.Uri = acp.Ptr(fileURI(path))

	if envelope != nil {
		block.Image.Meta = map[string]any{metaHandoffKey: envelope}
	}

	return block
}

func handoffPolicy(root string, limits ImageLimits) promptImagePolicy {
	return promptImagePolicy{limits: limits, handoffRoot: root}
}

// requireHandoffError pins the handoff error envelope: the existing input shape
// plus a human message naming the real cause, never a bare EOF.
func requireHandoffError(t *testing.T, err error, index int, errorValue, messageContains string) {
	t.Helper()

	var reqErr *acp.RequestError

	require.ErrorAs(t, err, &reqErr)
	require.Equal(t, -32602, reqErr.Code)

	data, ok := reqErr.Data.(map[string]any)
	require.True(t, ok)
	require.Len(t, data, 4)
	require.Equal(t, imageField, data[jsonFieldField])
	require.Equal(t, errorValue, data[jsonFieldError])
	require.Equal(t, index, data[keyIndex])

	message, ok := data[keyMessage].(string)
	require.True(t, ok)
	require.NotEmpty(t, message)
	require.NotEqual(t, io.EOF.Error(), message)
	require.Contains(t, message, messageContains)
}

func TestHandoffFormAcceptsPortableFormats(t *testing.T) {
	for _, test := range []struct {
		name     string
		mimeType string
	}{
		{name: "valid.png", mimeType: imageMIMEPNG},
		{name: "valid.jpg", mimeType: imageMIMEJPEG},
		{name: "valid.gif", mimeType: imageMIMEGIF},
		{name: "valid.webp", mimeType: imageMIMEWebP},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			data := imageFixture(t, test.name)
			path := writeHandoffFile(t, root, filepath.Join("session", "turn", test.name), data)

			input, err := promptInputWithPolicy(
				[]acp.ContentBlock{handoffBlock(path, test.mimeType, data)},
				handoffPolicy(root, applyOptions(nil).ImageLimits),
			)
			require.NoError(t, err)

			message, ok := input[keyMessage].(map[string]any)
			require.True(t, ok)
			content, ok := message[keyContent].([]map[string]any)
			require.True(t, ok)
			require.Len(t, content, 1)
			source, ok := content[0][keySource].(map[string]any)
			require.True(t, ok)
			require.Equal(t, base64.StdEncoding.EncodeToString(data), source[keyData])
		})
	}
}

// TestHandoffAcceptsASymlinkInsideTheRoot pins the legitimate link case: a link
// inside the read root naming a regular file inside the read root is read, not
// refused. The bytes are opened through the resolved target rather than the
// requested path, so the opener's refusal to follow a final symlink component
// guards against a swap without rejecting a host that lays its handoff directory
// out with links.
func TestHandoffAcceptsASymlinkInsideTheRoot(t *testing.T) {
	root := t.TempDir()
	data := imageFixture(t, "valid.png")
	target := writeHandoffFile(t, root, filepath.Join("session", "turn", "valid.png"), data)

	link := filepath.Join(root, "latest.png")
	require.NoError(t, os.Symlink(target, link))

	input, err := promptInputWithPolicy(
		[]acp.ContentBlock{handoffBlock(link, imageMIMEPNG, data)},
		handoffPolicy(root, applyOptions(nil).ImageLimits),
	)
	require.NoError(t, err)

	message, ok := input[keyMessage].(map[string]any)
	require.True(t, ok)
	content, ok := message[keyContent].([]map[string]any)
	require.True(t, ok)
	require.Len(t, content, 1)
	source, ok := content[0][keySource].(map[string]any)
	require.True(t, ok)
	require.Equal(t, base64.StdEncoding.EncodeToString(data), source[keyData])
}

func TestHandoffAndEmbeddedFormsBuildIdenticalNativeRequests(t *testing.T) {
	root := t.TempDir()
	data := imageFixture(t, "valid.png")
	path := writeHandoffFile(t, root, filepath.Join("session", "turn", "valid.png"), data)
	limits := applyOptions(nil).ImageLimits

	handoff, err := promptInputWithPolicy([]acp.ContentBlock{
		acp.TextBlock("look"),
		handoffBlock(path, imageMIMEPNG, data),
	}, handoffPolicy(root, limits))
	require.NoError(t, err)

	embedded, err := promptInputWithPolicy([]acp.ContentBlock{
		acp.TextBlock("look"),
		acp.ImageBlock(base64.StdEncoding.EncodeToString(data), imageMIMEPNG),
	}, promptImagePolicy{limits: limits})
	require.NoError(t, err)

	handoffJSON, err := json.Marshal(handoff)
	require.NoError(t, err)
	embeddedJSON, err := json.Marshal(embedded)
	require.NoError(t, err)
	require.JSONEq(t, string(embeddedJSON), string(handoffJSON))
}

func TestHandoffPathNeverReachesTheNativeRequest(t *testing.T) {
	root := t.TempDir()
	data := imageFixture(t, "valid.png")
	path := writeHandoffFile(t, root, filepath.Join("session", "turn", "secret-name.png"), data)

	input, err := promptInputWithPolicy(
		[]acp.ContentBlock{handoffBlock(path, imageMIMEPNG, data)},
		handoffPolicy(root, applyOptions(nil).ImageLimits),
	)
	require.NoError(t, err)

	encoded, err := json.Marshal(input)
	require.NoError(t, err)

	for _, leak := range []string{path, root, "secret-name.png", handoffDigest(data), metaHandoffKey, "file://"} {
		require.NotContains(t, string(encoded), leak)
	}
}

func TestHandoffFormSelection(t *testing.T) {
	root := t.TempDir()
	data := imageFixture(t, "valid.png")
	path := writeHandoffFile(t, root, "valid.png", data)
	limits := applyOptions(nil).ImageLimits

	t.Run("data wins over a handoff envelope", func(t *testing.T) {
		block := handoffBlockWithEnvelope(filepath.Join(root, "absent.png"), imageMIMEPNG, map[string]any{
			handoffFieldVersion:   handoffVersion,
			handoffFieldDigest:    handoffDigest(nil),
			handoffFieldSizeBytes: 0,
		})
		block.Image.Data = base64.StdEncoding.EncodeToString(data)

		input, err := promptInputWithPolicy([]acp.ContentBlock{block}, handoffPolicy(root, limits))
		require.NoError(t, err)

		message, ok := input[keyMessage].(map[string]any)
		require.True(t, ok)
		content, ok := message[keyContent].([]map[string]any)
		require.True(t, ok)
		source, ok := content[0][keySource].(map[string]any)
		require.True(t, ok)
		require.Equal(t, base64.StdEncoding.EncodeToString(data), source[keyData])
	})

	t.Run("empty data with no handoff intent stays missing_data", func(t *testing.T) {
		for _, block := range []acp.ContentBlock{
			acp.ImageBlock("", imageMIMEPNG),
			func() acp.ContentBlock {
				remote := acp.ImageBlock("", imageMIMEPNG)
				remote.Image.Uri = acp.Ptr("https://example.invalid/x.png")

				return remote
			}(),
			func() acp.ContentBlock {
				unparseable := acp.ImageBlock("", imageMIMEPNG)
				unparseable.Image.Uri = acp.Ptr("file://\x7f/x.png")

				return unparseable
			}(),
		} {
			_, err := promptInputWithPolicy([]acp.ContentBlock{block}, handoffPolicy(root, limits))
			requireInvalidParamsData(t, err, imageErrorData(0, imageErrorMissingData))
		}
	})

	t.Run("a file uri alone is handoff intent", func(t *testing.T) {
		block := acp.ImageBlock("", imageMIMEPNG)
		block.Image.Uri = acp.Ptr(fileURI(path))

		_, err := promptInputWithPolicy([]acp.ContentBlock{block}, handoffPolicy(root, limits))
		requireHandoffError(t, err, 0, imageErrorInvalidHandoff, "carries no acp-go.dev/handoff envelope")
	})

	t.Run("an envelope alone is handoff intent", func(t *testing.T) {
		block := acp.ImageBlock("", imageMIMEPNG)
		block.Image.Meta = map[string]any{metaHandoffKey: map[string]any{
			handoffFieldVersion:   handoffVersion,
			handoffFieldDigest:    handoffDigest(data),
			handoffFieldSizeBytes: len(data),
		}}

		_, err := promptInputWithPolicy([]acp.ContentBlock{block}, handoffPolicy(root, limits))
		requireHandoffError(t, err, 0, imageErrorInvalidHandoff, "carries no uri")
	})
}

func TestHandoffEnvelopeDefects(t *testing.T) {
	root := t.TempDir()
	data := imageFixture(t, "valid.png")
	path := writeHandoffFile(t, root, "valid.png", data)
	digest := handoffDigest(data)
	limits := applyOptions(nil).ImageLimits

	for _, test := range []struct {
		name     string
		envelope any
		contains string
	}{
		{name: "not an object", envelope: true, contains: "is not an object"},
		{
			name:     "unknown field",
			envelope: map[string]any{handoffFieldVersion: 1, handoffFieldDigest: digest, handoffFieldSizeBytes: len(data), "extra": 1},
			contains: `unknown field "extra"`,
		},
		{
			name:     "version missing",
			envelope: map[string]any{handoffFieldDigest: digest, handoffFieldSizeBytes: len(data)},
			contains: "version is missing or not an integer",
		},
		{
			name:     "version unsupported",
			envelope: map[string]any{handoffFieldVersion: 2, handoffFieldDigest: digest, handoffFieldSizeBytes: len(data)},
			contains: "version 2 is not supported",
		},
		{
			name:     "version fractional",
			envelope: map[string]any{handoffFieldVersion: 1.5, handoffFieldDigest: digest, handoffFieldSizeBytes: len(data)},
			contains: "version is not an integer",
		},
		{
			name:     "digest missing",
			envelope: map[string]any{handoffFieldVersion: 1, handoffFieldSizeBytes: len(data)},
			contains: "digest must be 64 lowercase hex characters",
		},
		{
			name:     "digest short",
			envelope: map[string]any{handoffFieldVersion: 1, handoffFieldDigest: digest[:63], handoffFieldSizeBytes: len(data)},
			contains: "digest must be 64 lowercase hex characters",
		},
		{
			name:     "digest uppercase",
			envelope: map[string]any{handoffFieldVersion: 1, handoffFieldDigest: strings.ToUpper(digest), handoffFieldSizeBytes: len(data)},
			contains: "digest must be 64 lowercase hex characters",
		},
		{
			name:     "digest not hex",
			envelope: map[string]any{handoffFieldVersion: 1, handoffFieldDigest: strings.Repeat("z", 64), handoffFieldSizeBytes: len(data)},
			contains: "digest must be 64 lowercase hex characters",
		},
		{
			name:     "sizeBytes missing",
			envelope: map[string]any{handoffFieldVersion: 1, handoffFieldDigest: digest},
			contains: "sizeBytes is missing or not an integer",
		},
		{
			name:     "sizeBytes fractional",
			envelope: map[string]any{handoffFieldVersion: 1, handoffFieldDigest: digest, handoffFieldSizeBytes: 1.5},
			contains: "sizeBytes is not an integer",
		},
		{
			name:     "sizeBytes negative",
			envelope: map[string]any{handoffFieldVersion: 1, handoffFieldDigest: digest, handoffFieldSizeBytes: -1},
			contains: "sizeBytes is negative",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := promptInputWithPolicy(
				[]acp.ContentBlock{handoffBlockWithEnvelope(path, imageMIMEPNG, test.envelope)},
				handoffPolicy(root, limits),
			)
			requireHandoffError(t, err, 0, imageErrorInvalidHandoff, test.contains)
		})
	}
}

func TestHandoffEnvelopeAcceptsTransportAndNativeIntegerShapes(t *testing.T) {
	root := t.TempDir()
	data := imageFixture(t, "valid.png")
	path := writeHandoffFile(t, root, "valid.png", data)

	for _, test := range []struct {
		name      string
		version   any
		sizeBytes any
	}{
		{name: "json numbers", version: float64(handoffVersion), sizeBytes: float64(len(data))},
		{name: "go ints", version: handoffVersion, sizeBytes: len(data)},
		{name: "go int64s", version: int64(handoffVersion), sizeBytes: int64(len(data))},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := promptInputWithPolicy([]acp.ContentBlock{
				handoffBlockWithEnvelope(path, imageMIMEPNG, map[string]any{
					handoffFieldVersion:   test.version,
					handoffFieldDigest:    handoffDigest(data),
					handoffFieldSizeBytes: test.sizeBytes,
				}),
			}, handoffPolicy(root, applyOptions(nil).ImageLimits))
			require.NoError(t, err)
		})
	}
}

func TestHandoffURIDefects(t *testing.T) {
	root := t.TempDir()
	data := imageFixture(t, "valid.png")
	envelope := map[string]any{
		handoffFieldVersion:   handoffVersion,
		handoffFieldDigest:    handoffDigest(data),
		handoffFieldSizeBytes: len(data),
	}

	for _, test := range []struct {
		name     string
		uri      *string
		contains string
	}{
		{name: "absent", uri: nil, contains: "carries no uri"},
		{name: "empty", uri: acp.Ptr(""), contains: "carries no uri"},
		{name: "unparseable", uri: acp.Ptr("file://\x7f/x.png"), contains: "is not parseable"},
		{name: "remote scheme", uri: acp.Ptr("https://example.invalid/x.png"), contains: `scheme "https" is not "file"`},
		{name: "remote host", uri: acp.Ptr("file://remote.invalid/x.png"), contains: `names remote host "remote.invalid"`},
		{name: "relative path", uri: acp.Ptr("file:relative/x.png"), contains: "is not absolute"},
	} {
		t.Run(test.name, func(t *testing.T) {
			block := acp.ImageBlock("", imageMIMEPNG)
			block.Image.Uri = test.uri
			block.Image.Meta = map[string]any{metaHandoffKey: envelope}

			_, err := promptInputWithPolicy(
				[]acp.ContentBlock{block},
				handoffPolicy(root, applyOptions(nil).ImageLimits),
			)
			requireHandoffError(t, err, 0, imageErrorInvalidHandoff, test.contains)
		})
	}
}

func TestHandoffAcceptsLocalhostURIHost(t *testing.T) {
	root := t.TempDir()
	data := imageFixture(t, "valid.png")
	path := writeHandoffFile(t, root, "valid.png", data)

	block := acp.ImageBlock("", imageMIMEPNG)
	block.Image.Uri = acp.Ptr("file://" + handoffURIHost + filepath.ToSlash(path))
	block.Image.Meta = map[string]any{metaHandoffKey: map[string]any{
		handoffFieldVersion:   handoffVersion,
		handoffFieldDigest:    handoffDigest(data),
		handoffFieldSizeBytes: len(data),
	}}

	_, err := promptInputWithPolicy(
		[]acp.ContentBlock{block},
		handoffPolicy(root, applyOptions(nil).ImageLimits),
	)
	require.NoError(t, err)
}

func TestHandoffRejectsUnsetReadRoot(t *testing.T) {
	root := t.TempDir()
	data := imageFixture(t, "valid.png")
	path := writeHandoffFile(t, root, "valid.png", data)

	_, err := promptInputWithPolicy(
		[]acp.ContentBlock{handoffBlock(path, imageMIMEPNG, data)},
		promptImagePolicy{limits: applyOptions(nil).ImageLimits},
	)
	requireHandoffError(t, err, 0, imageErrorInvalidHandoff, "no handoff read root is configured")
}

func TestHandoffPathNotAllowed(t *testing.T) {
	data := imageFixture(t, "valid.png")

	t.Run("outside the root", func(t *testing.T) {
		root := t.TempDir()
		outside := writeHandoffFile(t, t.TempDir(), "valid.png", data)

		_, err := promptInputWithPolicy(
			[]acp.ContentBlock{handoffBlock(outside, imageMIMEPNG, data)},
			handoffPolicy(root, applyOptions(nil).ImageLimits),
		)
		requireHandoffError(t, err, 0, imageErrorPathNotAllowed, "outside the configured read root")
	})

	t.Run("parent traversal out of the root", func(t *testing.T) {
		root := t.TempDir()
		outside := writeHandoffFile(t, filepath.Dir(root), "escaped.png", data)
		t.Cleanup(func() { _ = os.Remove(outside) })

		block := handoffBlockWithEnvelope(filepath.Join(root, "..", "escaped.png"), imageMIMEPNG, map[string]any{
			handoffFieldVersion:   handoffVersion,
			handoffFieldDigest:    handoffDigest(data),
			handoffFieldSizeBytes: len(data),
		})

		_, err := promptInputWithPolicy(
			[]acp.ContentBlock{block},
			handoffPolicy(root, applyOptions(nil).ImageLimits),
		)
		requireHandoffError(t, err, 0, imageErrorPathNotAllowed, "outside the configured read root")
	})

	t.Run("symlink escaping the root", func(t *testing.T) {
		root := t.TempDir()
		outside := writeHandoffFile(t, t.TempDir(), "valid.png", data)
		link := filepath.Join(root, "link.png")
		require.NoError(t, os.Symlink(outside, link))

		_, err := promptInputWithPolicy(
			[]acp.ContentBlock{handoffBlock(link, imageMIMEPNG, data)},
			handoffPolicy(root, applyOptions(nil).ImageLimits),
		)
		requireHandoffError(t, err, 0, imageErrorPathNotAllowed, "resolves outside the configured read root")
	})

	t.Run("not a regular file", func(t *testing.T) {
		root := t.TempDir()
		nested := filepath.Join(root, "nested")
		require.NoError(t, os.MkdirAll(nested, 0o700))

		_, err := promptInputWithPolicy(
			[]acp.ContentBlock{handoffBlock(nested, imageMIMEPNG, data)},
			handoffPolicy(root, applyOptions(nil).ImageLimits),
		)
		requireHandoffError(t, err, 0, imageErrorPathNotAllowed, "not a regular file")
	})

	t.Run("the read root itself is contained but not readable as a file", func(t *testing.T) {
		root := t.TempDir()

		_, err := promptInputWithPolicy(
			[]acp.ContentBlock{handoffBlock(root, imageMIMEPNG, data)},
			handoffPolicy(root, applyOptions(nil).ImageLimits),
		)
		requireHandoffError(t, err, 0, imageErrorPathNotAllowed, "not a regular file")
	})

	t.Run("unresolvable read root", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "absent-root")

		_, err := promptInputWithPolicy(
			[]acp.ContentBlock{handoffBlock(filepath.Join(root, "valid.png"), imageMIMEPNG, data)},
			handoffPolicy(root, applyOptions(nil).ImageLimits),
		)
		requireHandoffError(t, err, 0, imageErrorPathNotAllowed, "read root cannot be resolved")
	})

	t.Run("path resolution failure that is not absence", func(t *testing.T) {
		root := t.TempDir()
		path := writeHandoffFile(t, root, "valid.png", data)

		restore := evalSymlinks
		evalSymlinks = func(name string) (string, error) {
			if name == filepath.Clean(root) {
				return restore(name)
			}

			return "", errors.New("too many levels of symbolic links")
		}
		t.Cleanup(func() { evalSymlinks = restore })

		_, err := promptInputWithPolicy(
			[]acp.ContentBlock{handoffBlock(path, imageMIMEPNG, data)},
			handoffPolicy(root, applyOptions(nil).ImageLimits),
		)
		requireHandoffError(t, err, 0, imageErrorPathNotAllowed, "cannot be resolved")
	})

	t.Run("inspection failure that is not absence", func(t *testing.T) {
		root := t.TempDir()
		path := writeHandoffFile(t, root, "valid.png", data)

		restore := statHandoffFile
		statHandoffFile = func(string) (os.FileInfo, error) { return nil, errors.New("permission denied") }
		t.Cleanup(func() { statHandoffFile = restore })

		_, err := promptInputWithPolicy(
			[]acp.ContentBlock{handoffBlock(path, imageMIMEPNG, data)},
			handoffPolicy(root, applyOptions(nil).ImageLimits),
		)
		requireHandoffError(t, err, 0, imageErrorPathNotAllowed, "cannot be inspected")
	})
}

func TestHandoffMissingFile(t *testing.T) {
	data := imageFixture(t, "valid.png")

	t.Run("vanished path inside the root", func(t *testing.T) {
		root := t.TempDir()

		_, err := promptInputWithPolicy(
			[]acp.ContentBlock{handoffBlock(filepath.Join(root, "turn", "gone.png"), imageMIMEPNG, data)},
			handoffPolicy(root, applyOptions(nil).ImageLimits),
		)
		requireHandoffError(t, err, 0, imageErrorMissingFile, "does not exist")
	})

	t.Run("vanished between resolution and inspection", func(t *testing.T) {
		root := t.TempDir()
		path := writeHandoffFile(t, root, "valid.png", data)

		restore := statHandoffFile
		statHandoffFile = func(string) (os.FileInfo, error) { return nil, os.ErrNotExist }
		t.Cleanup(func() { statHandoffFile = restore })

		_, err := promptInputWithPolicy(
			[]acp.ContentBlock{handoffBlock(path, imageMIMEPNG, data)},
			handoffPolicy(root, applyOptions(nil).ImageLimits),
		)
		requireHandoffError(t, err, 0, imageErrorMissingFile, "does not exist")
	})

	t.Run("unopenable file", func(t *testing.T) {
		root := t.TempDir()
		path := writeHandoffFile(t, root, "valid.png", data)

		restore := openHandoffFile
		openHandoffFile = func(string) (handoffFile, error) { return nil, os.ErrPermission }
		t.Cleanup(func() { openHandoffFile = restore })

		_, err := promptInputWithPolicy(
			[]acp.ContentBlock{handoffBlock(path, imageMIMEPNG, data)},
			handoffPolicy(root, applyOptions(nil).ImageLimits),
		)
		requireHandoffError(t, err, 0, imageErrorMissingFile, "cannot be opened")
	})

	t.Run("unreadable file", func(t *testing.T) {
		root := t.TempDir()
		path := writeHandoffFile(t, root, "valid.png", data)

		restore := openHandoffFile
		openHandoffFile = func(name string) (handoffFile, error) {
			opened, err := restore(name)
			require.NoError(t, err)

			return unreadableHandoffFile{handoffFile: opened}, nil
		}
		t.Cleanup(func() { openHandoffFile = restore })

		_, err := promptInputWithPolicy(
			[]acp.ContentBlock{handoffBlock(path, imageMIMEPNG, data)},
			handoffPolicy(root, applyOptions(nil).ImageLimits),
		)
		requireHandoffError(t, err, 0, imageErrorMissingFile, "cannot be read")
	})
}

// unreadableHandoffFile keeps a real descriptor's mode and identity while failing
// every read, so the read-failure branch is reached after the identity check.
// TestHandoffFileSwappedAfterResolutionIsRejected pins the descriptor check: the
// bytes read must come from the same regular file the path resolution admitted,
// so a path replaced by a symlink, a FIFO, or another file in that window is
// rejected instead of read.
func TestHandoffFileSwappedAfterResolutionIsRejected(t *testing.T) {
	root := t.TempDir()
	data := imageFixture(t, "valid.png")
	path := writeHandoffFile(t, root, "valid.png", data)

	restore := openHandoffFile
	openHandoffFile = func(name string) (handoffFile, error) {
		opened, err := restore(name)
		require.NoError(t, err)

		return swappedHandoffFile{handoffFile: opened}, nil
	}
	t.Cleanup(func() { openHandoffFile = restore })

	_, err := promptInputWithPolicy(
		[]acp.ContentBlock{handoffBlock(path, imageMIMEPNG, data)},
		handoffPolicy(root, applyOptions(nil).ImageLimits),
	)
	requireHandoffError(t, err, 0, imageErrorPathNotAllowed, "not the regular file that was resolved")
}

func TestHandoffFileDescriptorInspectionFailureIsMissingFile(t *testing.T) {
	root := t.TempDir()
	data := imageFixture(t, "valid.png")
	path := writeHandoffFile(t, root, "valid.png", data)

	restore := openHandoffFile
	openHandoffFile = func(name string) (handoffFile, error) {
		opened, err := restore(name)
		require.NoError(t, err)

		return uninspectableHandoffFile{handoffFile: opened}, nil
	}
	t.Cleanup(func() { openHandoffFile = restore })

	_, err := promptInputWithPolicy(
		[]acp.ContentBlock{handoffBlock(path, imageMIMEPNG, data)},
		handoffPolicy(root, applyOptions(nil).ImageLimits),
	)
	requireHandoffError(t, err, 0, imageErrorMissingFile, "cannot be inspected")
}

// TestHandoffCausesNameNoHostPath pins that a verdict a client receives carries
// the real operating-system cause and never the path it names.
func TestHandoffCausesNameNoHostPath(t *testing.T) {
	root := t.TempDir()
	absent := filepath.Join(root, "session", "gone.png")

	_, err := promptInputWithPolicy(
		[]acp.ContentBlock{handoffBlock(absent, imageMIMEPNG, imageFixture(t, "valid.png"))},
		handoffPolicy(root, applyOptions(nil).ImageLimits),
	)

	var reqErr *acp.RequestError

	require.ErrorAs(t, err, &reqErr)
	require.NotContains(t, reqErr.Error(), root)
	require.NotContains(t, reqErr.Error(), absent)
	require.Contains(t, reqErr.Error(), "does not exist")
}

type uninspectableHandoffFile struct {
	handoffFile
}

func (uninspectableHandoffFile) Stat() (os.FileInfo, error) {
	return nil, errors.New("stale file handle")
}

type unreadableHandoffFile struct {
	handoffFile
}

func (unreadableHandoffFile) Read([]byte) (int, error) { return 0, errors.New("input/output error") }

// swappedHandoffFile reports a mode and identity that do not match the file the
// path resolved to, standing in for a path replaced between resolution and read.
type swappedHandoffFile struct {
	handoffFile
}

func (swappedHandoffFile) Stat() (os.FileInfo, error) { return swappedFileInfo{}, nil }

type swappedFileInfo struct{ os.FileInfo }

func (swappedFileInfo) Mode() os.FileMode { return os.ModeNamedPipe }

func TestHandoffDigestMismatch(t *testing.T) {
	data := imageFixture(t, "valid.png")

	t.Run("tampered bytes", func(t *testing.T) {
		root := t.TempDir()

		tampered := append([]byte(nil), data...)
		tampered[len(tampered)-1] ^= 0xff
		path := writeHandoffFile(t, root, "valid.png", tampered)

		_, err := promptInputWithPolicy(
			[]acp.ContentBlock{handoffBlock(path, imageMIMEPNG, data)},
			handoffPolicy(root, applyOptions(nil).ImageLimits),
		)
		requireHandoffError(t, err, 0, imageErrorDigestMismatch, "do not hash to the declared digest")
	})

	t.Run("declared size disagrees", func(t *testing.T) {
		root := t.TempDir()
		path := writeHandoffFile(t, root, "valid.png", data)

		_, err := promptInputWithPolicy([]acp.ContentBlock{
			handoffBlockWithEnvelope(path, imageMIMEPNG, map[string]any{
				handoffFieldVersion:   handoffVersion,
				handoffFieldDigest:    handoffDigest(data),
				handoffFieldSizeBytes: len(data) - 1,
			}),
		}, handoffPolicy(root, applyOptions(nil).ImageLimits))
		requireHandoffError(t, err, 0, imageErrorDigestMismatch, "envelope declares")
	})
}

func TestHandoffGateChainMirrorsTheEmbeddedForm(t *testing.T) {
	root := t.TempDir()
	validPNG := imageFixture(t, "valid.png")

	for _, test := range []struct {
		name     string
		fixture  string
		mimeType string
		want     string
	}{
		{name: "unrecognized declaration", fixture: "valid.png", mimeType: "IMAGE/PNG", want: imageErrorInvalidMediaType},
		{name: "media type mismatch", fixture: "mismatch.png", mimeType: imageMIMEPNG, want: imageErrorMediaTypeMismatch},
		{name: "truncated dimensions", fixture: "truncated.png", mimeType: imageMIMEPNG, want: imageErrorInvalidDimensions},
		{name: "animated gif", fixture: "animated.gif", mimeType: imageMIMEGIF, want: imageErrorAnimatedNotSupported},
		{name: "animated webp", fixture: "animated.webp", mimeType: imageMIMEWebP, want: imageErrorAnimatedNotSupported},
		{name: "animated png", fixture: "animated-apng.png", mimeType: imageMIMEPNG, want: imageErrorAnimatedNotSupported},
		{name: "declared type mismatch", fixture: "valid.png", mimeType: imageMIMEJPEG, want: imageErrorMediaTypeMismatch},
	} {
		t.Run(test.name, func(t *testing.T) {
			data := imageFixture(t, test.fixture)
			path := writeHandoffFile(t, root, test.name+"-"+test.fixture, data)

			_, err := promptInputWithPolicy(
				[]acp.ContentBlock{handoffBlock(path, test.mimeType, data)},
				handoffPolicy(root, applyOptions(nil).ImageLimits),
			)
			requireInvalidParamsData(t, err, imageErrorData(0, test.want))
		})
	}

	t.Run("per image limit reports the real size", func(t *testing.T) {
		path := writeHandoffFile(t, root, "per-image.png", validPNG)
		limits := applyOptions(nil).ImageLimits
		limits.MaxInputBytesPerImage = int64(len(validPNG) - 1)

		_, err := promptInputWithPolicy(
			[]acp.ContentBlock{handoffBlock(path, imageMIMEPNG, validPNG)},
			handoffPolicy(root, limits),
		)
		requireInvalidParamsData(t, err, imageSizeErrorData(
			0,
			imageErrorTooLarge,
			int64(len(validPNG)),
			limits.MaxInputBytesPerImage,
		))
	})

	t.Run("handoff bytes count toward the per-prompt aggregate", func(t *testing.T) {
		path := writeHandoffFile(t, root, "aggregate.png", validPNG)
		limits := applyOptions(nil).ImageLimits
		limits.MaxInputBytesPerPrompt = int64(len(validPNG)*2 - 1)

		_, err := promptInputWithPolicy([]acp.ContentBlock{
			acp.ImageBlock(base64.StdEncoding.EncodeToString(validPNG), imageMIMEPNG),
			handoffBlock(path, imageMIMEPNG, validPNG),
		}, handoffPolicy(root, limits))
		requireInvalidParamsData(t, err, imageSizeErrorData(
			1,
			imageErrorTooLarge,
			int64(len(validPNG)*2),
			limits.MaxInputBytesPerPrompt,
		))
	})

	t.Run("dimension envelope", func(t *testing.T) {
		wide := append([]byte(nil), validPNG...)
		binary.BigEndian.PutUint32(wide[16:20], ampNativeMaxImageDimension+1)
		path := writeHandoffFile(t, root, "wide.png", wide)

		_, err := promptInputWithPolicy(
			[]acp.ContentBlock{handoffBlock(path, imageMIMEPNG, wide)},
			handoffPolicy(root, applyOptions(nil).ImageLimits),
		)
		requireInvalidParamsData(t, err, imageErrorData(0, imageErrorNativeEnvelope))
	})
}

func TestHandoffFileBeyondTheReadBoundIsRejectedUnverified(t *testing.T) {
	root := t.TempDir()

	oversize := make([]byte, ampNativeMaxImageBytes+1)
	copy(oversize, imageFixture(t, "valid.png"))
	path := writeHandoffFile(t, root, "oversize.png", oversize)

	// The digest is deliberately wrong: a file that exceeds the read bound cannot
	// be verified, and it must still be rejected on size rather than admitted.
	block := handoffBlockWithEnvelope(path, imageMIMEPNG, map[string]any{
		handoffFieldVersion:   handoffVersion,
		handoffFieldDigest:    handoffDigest(nil),
		handoffFieldSizeBytes: len(oversize),
	})

	_, err := promptInputWithPolicy(
		[]acp.ContentBlock{block},
		handoffPolicy(root, applyOptions(nil).ImageLimits),
	)
	requireInvalidParamsData(t, err, imageSizeErrorData(
		0,
		imageErrorNativeEnvelope,
		int64(len(oversize)),
		ampNativeMaxImageBytes,
	))
}

func TestHandoffFileFarBeyondTheReadBoundReportsItsRealSize(t *testing.T) {
	root := t.TempDir()

	oversize := make([]byte, ampNativeMaxImageBytes+4096)
	copy(oversize, imageFixture(t, "valid.png"))
	path := writeHandoffFile(t, root, "oversize.png", oversize)

	_, err := promptInputWithPolicy(
		[]acp.ContentBlock{handoffBlock(path, imageMIMEPNG, oversize)},
		handoffPolicy(root, applyOptions(nil).ImageLimits),
	)
	requireInvalidParamsData(t, err, imageSizeErrorData(
		0,
		imageErrorNativeEnvelope,
		int64(len(oversize)),
		ampNativeMaxImageBytes,
	))
}

func TestHandoffReadBoundTracksTheTighterConfiguredLimit(t *testing.T) {
	root := t.TempDir()
	validPNG := imageFixture(t, "valid.png")

	oversize := make([]byte, len(validPNG)+1)
	copy(oversize, validPNG)
	path := writeHandoffFile(t, root, "oversize.png", oversize)

	limits := applyOptions(nil).ImageLimits
	limits.MaxInputBytesPerImage = int64(len(validPNG))

	_, err := promptInputWithPolicy(
		[]acp.ContentBlock{handoffBlock(path, imageMIMEPNG, oversize)},
		handoffPolicy(root, limits),
	)
	requireInvalidParamsData(t, err, imageSizeErrorData(
		0,
		imageErrorTooLarge,
		int64(len(oversize)),
		limits.MaxInputBytesPerImage,
	))
}

func TestEffectiveInputBytesPerImageIsTheTightestBound(t *testing.T) {
	for _, test := range []struct {
		name       string
		configured int64
		want       int64
	}{
		{name: "default policy yields the native envelope", configured: defaultImageLimitBytes, want: ampNativeMaxImageBytes},
		{name: "unbounded configuration yields the native envelope", configured: 0, want: ampNativeMaxImageBytes},
		{name: "tighter configuration wins", configured: 1024, want: 1024},
		{name: "equal configuration yields the native envelope", configured: ampNativeMaxImageBytes, want: ampNativeMaxImageBytes},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, effectiveInputBytesPerImage(ImageLimits{MaxInputBytesPerImage: test.configured}))
		})
	}
}
