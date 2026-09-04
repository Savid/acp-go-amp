package ampacp

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

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

// fileURI spells a host path as a file URI. RFC 8089 puts the path after an
// empty authority, so a Windows path whose first component is a drive letter
// gains the separating slash a Unix path already carries.
func fileURI(path string) string {
	return fileURIWithHost("", path)
}

// fileURIWithHost spells a host path as a file URI under a named authority.
func fileURIWithHost(host, path string) string {
	slashed := filepath.ToSlash(path)
	if !strings.HasPrefix(slashed, "/") {
		slashed = "/" + slashed
	}

	return "file://" + host + slashed
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

func TestHandoffCapabilityScalar(t *testing.T) {
	require.NotContains(t, initializeMeta(t), "acp-go.dev/handoff")

	meta := initializeMeta(t, WithInputHandoffRoot(t.TempDir()))
	require.Equal(t, map[string]any{"version": 1}, meta["acp-go.dev/handoff"])
}

// requireHandoffError pins one handoff verdict exactly: the input error shape
// plus the whole message, which must be the declared constant and never a string
// built from something the adapter observed.
func requireHandoffError(t *testing.T, err error, index int, errorValue, message string) {
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
	require.Equal(t, message, data[keyMessage])
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
				t.Context(),
				[]acp.ContentBlock{handoffBlock(path, test.mimeType, data)},
				handoffPolicy(root, applyOptions(nil).ImageLimits),
			)
			require.NoError(t, err)
			require.Equal(t, base64.StdEncoding.EncodeToString(data), promptContentBase64(t, input))
		})
	}
}

func TestHandoffAndEmbeddedFormsBuildIdenticalNativeRequests(t *testing.T) {
	root := t.TempDir()
	data := imageFixture(t, "valid.png")
	path := writeHandoffFile(t, root, filepath.Join("session", "turn", "valid.png"), data)
	limits := applyOptions(nil).ImageLimits

	handoff, err := promptInputWithPolicy(t.Context(), []acp.ContentBlock{
		acp.TextBlock("look"),
		handoffBlock(path, imageMIMEPNG, data),
	}, handoffPolicy(root, limits))
	require.NoError(t, err)

	embedded, err := promptInputWithPolicy(t.Context(), []acp.ContentBlock{
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
		t.Context(),
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

		input, err := promptInputWithPolicy(t.Context(), []acp.ContentBlock{block}, handoffPolicy(root, limits))
		require.NoError(t, err)
		require.Equal(t, base64.StdEncoding.EncodeToString(data), promptContentBase64(t, input))
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
				unparsable := acp.ImageBlock("", imageMIMEPNG)
				unparsable.Image.Uri = acp.Ptr("file://\x7f/x.png")

				return unparsable
			}(),
		} {
			_, err := promptInputWithPolicy(t.Context(), []acp.ContentBlock{block}, handoffPolicy(root, limits))
			requireInvalidParamsData(t, err, imageErrorData(0, imageErrorMissingData))
		}
	})

	t.Run("a file uri alone is handoff intent", func(t *testing.T) {
		block := acp.ImageBlock("", imageMIMEPNG)
		block.Image.Uri = acp.Ptr(fileURI(path))

		_, err := promptInputWithPolicy(t.Context(), []acp.ContentBlock{block}, handoffPolicy(root, limits))
		requireHandoffError(t, err, 0, imageErrorInvalidHandoff, handoffEnvelopeAbsentMessage)
	})

	t.Run("an envelope alone is handoff intent", func(t *testing.T) {
		block := acp.ImageBlock("", imageMIMEPNG)
		block.Image.Meta = map[string]any{metaHandoffKey: map[string]any{
			handoffFieldVersion:   handoffVersion,
			handoffFieldDigest:    handoffDigest(data),
			handoffFieldSizeBytes: len(data),
		}}

		_, err := promptInputWithPolicy(t.Context(), []acp.ContentBlock{block}, handoffPolicy(root, limits))
		requireHandoffError(t, err, 0, imageErrorInvalidHandoff, handoffURIAbsentMessage)
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
		message  string
	}{
		{name: "not an object", envelope: true, message: handoffEnvelopeNotObjectMessage},
		{
			name:     "unknown field",
			envelope: map[string]any{handoffFieldVersion: 1, handoffFieldDigest: digest, handoffFieldSizeBytes: len(data), "extra": 1},
			message:  handoffEnvelopeUnknownFieldMessage,
		},
		{
			name:     "version missing",
			envelope: map[string]any{handoffFieldDigest: digest, handoffFieldSizeBytes: len(data)},
			message:  handoffVersionInvalidMessage,
		},
		{
			name:     "version fractional",
			envelope: map[string]any{handoffFieldVersion: 1.5, handoffFieldDigest: digest, handoffFieldSizeBytes: len(data)},
			message:  handoffVersionInvalidMessage,
		},
		{
			name:     "version zero",
			envelope: map[string]any{handoffFieldVersion: 0, handoffFieldDigest: digest, handoffFieldSizeBytes: len(data)},
			message:  handoffVersionUnsupportedMessage,
		},
		{
			name:     "version unsupported",
			envelope: map[string]any{handoffFieldVersion: 2, handoffFieldDigest: digest, handoffFieldSizeBytes: len(data)},
			message:  handoffVersionUnsupportedMessage,
		},
		{
			name:     "digest missing",
			envelope: map[string]any{handoffFieldVersion: 1, handoffFieldSizeBytes: len(data)},
			message:  handoffDigestInvalidMessage,
		},
		{
			name:     "digest short",
			envelope: map[string]any{handoffFieldVersion: 1, handoffFieldDigest: digest[:63], handoffFieldSizeBytes: len(data)},
			message:  handoffDigestInvalidMessage,
		},
		{
			name:     "digest uppercase",
			envelope: map[string]any{handoffFieldVersion: 1, handoffFieldDigest: strings.ToUpper(digest), handoffFieldSizeBytes: len(data)},
			message:  handoffDigestInvalidMessage,
		},
		{
			name:     "digest not hex",
			envelope: map[string]any{handoffFieldVersion: 1, handoffFieldDigest: strings.Repeat("z", 64), handoffFieldSizeBytes: len(data)},
			message:  handoffDigestInvalidMessage,
		},
		{
			name:     "sizeBytes missing",
			envelope: map[string]any{handoffFieldVersion: 1, handoffFieldDigest: digest},
			message:  handoffSizeBytesInvalidMessage,
		},
		{
			name:     "sizeBytes fractional",
			envelope: map[string]any{handoffFieldVersion: 1, handoffFieldDigest: digest, handoffFieldSizeBytes: 1.5},
			message:  handoffSizeBytesInvalidMessage,
		},
		{
			name:     "sizeBytes negative",
			envelope: map[string]any{handoffFieldVersion: 1, handoffFieldDigest: digest, handoffFieldSizeBytes: -1},
			message:  handoffSizeBytesInvalidMessage,
		},
		{
			name: "sizeBytes at two to the sixty-three",
			envelope: map[string]any{
				handoffFieldVersion: 1, handoffFieldDigest: digest, handoffFieldSizeBytes: handoffSizeBytesExclusiveMax,
			},
			message: handoffSizeBytesInvalidMessage,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := promptInputWithPolicy(
				t.Context(),
				[]acp.ContentBlock{handoffBlockWithEnvelope(path, imageMIMEPNG, test.envelope)},
				handoffPolicy(root, limits),
			)
			requireHandoffError(t, err, 0, imageErrorInvalidHandoff, test.message)
		})
	}
}

// TestHandoffEnvelopeSizeIsJudgedByTheByteGates pins that a size a host may
// legally declare but no file can hold is not an envelope defect: 2^53 is below
// the int64 range and is judged by the per-image bound like any other size.
func TestHandoffEnvelopeSizeIsJudgedByTheByteGates(t *testing.T) {
	root := t.TempDir()
	data := imageFixture(t, "valid.png")
	path := writeHandoffFile(t, root, "valid.png", data)
	limits := applyOptions(nil).ImageLimits

	_, err := promptInputWithPolicy(t.Context(), []acp.ContentBlock{
		handoffBlockWithEnvelope(path, imageMIMEPNG, map[string]any{
			handoffFieldVersion:   handoffVersion,
			handoffFieldDigest:    handoffDigest(data),
			handoffFieldSizeBytes: float64(1 << 53),
		}),
	}, handoffPolicy(root, limits))
	requireInvalidParamsData(t, err, imageSizeErrorData(
		0, imageErrorTooLarge, 1<<53, limits.MaxInputBytesPerImage,
	))
}

func TestHandoffEnvelopeAcceptsNumbersFromADecoder(t *testing.T) {
	root := t.TempDir()
	data := imageFixture(t, "valid.png")
	path := writeHandoffFile(t, root, "valid.png", data)

	raw := fmt.Sprintf(`{"version":1,"digest":%q,"sizeBytes":%d}`, handoffDigest(data), len(data))

	// The pinned SDK decodes envelope numbers to float64, but a decoder asked for
	// json.Number is one upstream flag away and must validate identically.
	for _, useNumber := range []bool{false, true} {
		decoder := json.NewDecoder(strings.NewReader(raw))
		if useNumber {
			decoder.UseNumber()
		}

		var envelope map[string]any

		require.NoError(t, decoder.Decode(&envelope))

		_, err := promptInputWithPolicy(
			t.Context(),
			[]acp.ContentBlock{handoffBlockWithEnvelope(path, imageMIMEPNG, envelope)},
			handoffPolicy(root, applyOptions(nil).ImageLimits),
		)
		require.NoError(t, err, "useNumber=%v", useNumber)
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
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := promptInputWithPolicy(t.Context(), []acp.ContentBlock{
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
		name    string
		uri     *string
		message string
	}{
		{name: "absent", uri: nil, message: handoffURIAbsentMessage},
		{name: "empty", uri: acp.Ptr(""), message: handoffURIAbsentMessage},
		{name: "unparsable", uri: acp.Ptr("file://\x7f/x.png"), message: handoffURIUnparsableMessage},
		{name: "remote scheme", uri: acp.Ptr("https://example.invalid/x.png"), message: handoffURISchemeMessage},
		{name: "remote host", uri: acp.Ptr("file://remote.invalid/x.png"), message: handoffURIRemoteHostMessage},
		{name: "relative path", uri: acp.Ptr("file:relative/x.png"), message: handoffURIRelativeMessage},
	} {
		t.Run(test.name, func(t *testing.T) {
			block := acp.ImageBlock("", imageMIMEPNG)
			block.Image.Uri = test.uri
			block.Image.Meta = map[string]any{metaHandoffKey: envelope}

			_, err := promptInputWithPolicy(
				t.Context(),
				[]acp.ContentBlock{block},
				handoffPolicy(root, applyOptions(nil).ImageLimits),
			)
			requireHandoffError(t, err, 0, imageErrorInvalidHandoff, test.message)
		})
	}
}

func TestHandoffAcceptsLocalhostURIHost(t *testing.T) {
	root := t.TempDir()
	data := imageFixture(t, "valid.png")
	path := writeHandoffFile(t, root, "valid.png", data)

	block := acp.ImageBlock("", imageMIMEPNG)
	block.Image.Uri = acp.Ptr(fileURIWithHost(handoffURIHost, path))
	block.Image.Meta = map[string]any{metaHandoffKey: map[string]any{
		handoffFieldVersion:   handoffVersion,
		handoffFieldDigest:    handoffDigest(data),
		handoffFieldSizeBytes: len(data),
	}}

	_, err := promptInputWithPolicy(
		t.Context(),
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
		t.Context(),
		[]acp.ContentBlock{handoffBlock(path, imageMIMEPNG, data)},
		promptImagePolicy{limits: applyOptions(nil).ImageLimits},
	)
	requireHandoffError(t, err, 0, imageErrorInvalidHandoff, handoffRootUnsetMessage)
}

func TestHandoffPathNotAllowed(t *testing.T) {
	data := imageFixture(t, "valid.png")

	t.Run("outside the root", func(t *testing.T) {
		root := t.TempDir()
		outside := writeHandoffFile(t, t.TempDir(), "valid.png", data)

		_, err := promptInputWithPolicy(
			t.Context(),
			[]acp.ContentBlock{handoffBlock(outside, imageMIMEPNG, data)},
			handoffPolicy(root, applyOptions(nil).ImageLimits),
		)
		requireHandoffError(t, err, 0, imageErrorPathNotAllowed, handoffOutsideRootMessage)
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
			t.Context(),
			[]acp.ContentBlock{block},
			handoffPolicy(root, applyOptions(nil).ImageLimits),
		)
		requireHandoffError(t, err, 0, imageErrorPathNotAllowed, handoffOutsideRootMessage)
	})

	t.Run("percent-encoded traversal out of the root", func(t *testing.T) {
		root := t.TempDir()
		outside := writeHandoffFile(t, t.TempDir(), "secret.png", data)

		block := acp.ImageBlock("", imageMIMEPNG)
		block.Image.Uri = acp.Ptr(fileURI(root) + "/%2e%2e/" +
			filepath.Base(filepath.Dir(outside)) + "/secret.png")
		block.Image.Meta = map[string]any{metaHandoffKey: map[string]any{
			handoffFieldVersion:   handoffVersion,
			handoffFieldDigest:    handoffDigest(data),
			handoffFieldSizeBytes: len(data),
		}}

		_, err := promptInputWithPolicy(
			t.Context(),
			[]acp.ContentBlock{block},
			handoffPolicy(root, applyOptions(nil).ImageLimits),
		)
		requireHandoffError(t, err, 0, imageErrorPathNotAllowed, handoffOutsideRootMessage)
	})

	t.Run("not a regular file", func(t *testing.T) {
		root := t.TempDir()
		nested := filepath.Join(root, "nested")
		require.NoError(t, os.MkdirAll(nested, 0o700))

		_, err := promptInputWithPolicy(
			t.Context(),
			[]acp.ContentBlock{handoffBlock(nested, imageMIMEPNG, data)},
			handoffPolicy(root, applyOptions(nil).ImageLimits),
		)
		requireHandoffError(t, err, 0, imageErrorPathNotAllowed, handoffNotRegularMessage)
	})

	t.Run("the read root itself is contained but not readable as a file", func(t *testing.T) {
		root := t.TempDir()

		_, err := promptInputWithPolicy(
			t.Context(),
			[]acp.ContentBlock{handoffBlock(root, imageMIMEPNG, data)},
			handoffPolicy(root, applyOptions(nil).ImageLimits),
		)
		requireHandoffError(t, err, 0, imageErrorPathNotAllowed, handoffNotRegularMessage)
	})

	t.Run("unopenable read root", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "absent-root")

		_, err := promptInputWithPolicy(
			t.Context(),
			[]acp.ContentBlock{handoffBlock(filepath.Join(root, "valid.png"), imageMIMEPNG, data)},
			handoffPolicy(root, applyOptions(nil).ImageLimits),
		)
		requireHandoffError(t, err, 0, imageErrorPathNotAllowed, handoffRootUnopenableMessage)
	})

	t.Run("open failure that is not absence", func(t *testing.T) {
		root := t.TempDir()
		path := writeHandoffFile(t, root, "valid.png", data)

		restore := openHandoffFile
		openHandoffFile = func(*os.Root, string) (handoffFile, error) { return nil, os.ErrPermission }
		t.Cleanup(func() { openHandoffFile = restore })

		_, err := promptInputWithPolicy(
			t.Context(),
			[]acp.ContentBlock{handoffBlock(path, imageMIMEPNG, data)},
			handoffPolicy(root, applyOptions(nil).ImageLimits),
		)
		requireHandoffError(t, err, 0, imageErrorPathNotAllowed, handoffUnopenableMessage)
	})
}

func TestHandoffMissingFile(t *testing.T) {
	data := imageFixture(t, "valid.png")

	t.Run("vanished path inside the root", func(t *testing.T) {
		root := t.TempDir()

		_, err := promptInputWithPolicy(
			t.Context(),
			[]acp.ContentBlock{handoffBlock(filepath.Join(root, "turn", "gone.png"), imageMIMEPNG, data)},
			handoffPolicy(root, applyOptions(nil).ImageLimits),
		)
		requireHandoffError(t, err, 0, imageErrorMissingFile, handoffFileAbsentMessage)
	})

	t.Run("uninspectable descriptor", func(t *testing.T) {
		root := t.TempDir()
		path := writeHandoffFile(t, root, "valid.png", data)

		restore := openHandoffFile
		openHandoffFile = func(*os.Root, string) (handoffFile, error) {
			return failingHandoffFile{statErr: errors.New("stale file handle")}, nil
		}
		t.Cleanup(func() { openHandoffFile = restore })

		_, err := promptInputWithPolicy(
			t.Context(),
			[]acp.ContentBlock{handoffBlock(path, imageMIMEPNG, data)},
			handoffPolicy(root, applyOptions(nil).ImageLimits),
		)
		requireHandoffError(t, err, 0, imageErrorMissingFile, handoffUninspectableMessage)
	})

	t.Run("unreadable descriptor", func(t *testing.T) {
		root := t.TempDir()
		path := writeHandoffFile(t, root, "valid.png", data)

		restore := openHandoffFile
		openHandoffFile = func(open *os.Root, rel string) (handoffFile, error) {
			info, err := open.Stat(rel)
			require.NoError(t, err)

			return failingHandoffFile{readErr: errors.New("input/output error"), info: info}, nil
		}
		t.Cleanup(func() { openHandoffFile = restore })

		_, err := promptInputWithPolicy(
			t.Context(),
			[]acp.ContentBlock{handoffBlock(path, imageMIMEPNG, data)},
			handoffPolicy(root, applyOptions(nil).ImageLimits),
		)
		requireHandoffError(t, err, 0, imageErrorMissingFile, handoffUnreadableMessage)
	})
}

// failingHandoffFile is an opened handoff file whose inspection or read fails,
// standing in for the descriptor-level failures a real filesystem only produces
// under a race with the host that owns the file.
type failingHandoffFile struct {
	readErr error
	statErr error
	info    os.FileInfo
}

func (f failingHandoffFile) Read([]byte) (int, error) { return 0, f.readErr }

func (failingHandoffFile) Close() error { return nil }

func (f failingHandoffFile) Stat() (os.FileInfo, error) {
	if f.statErr != nil {
		return nil, f.statErr
	}

	return f.info, nil
}

func TestHandoffDigestMismatch(t *testing.T) {
	data := imageFixture(t, "valid.png")

	t.Run("tampered bytes", func(t *testing.T) {
		root := t.TempDir()

		tampered := append([]byte(nil), data...)
		tampered[len(tampered)-1] ^= 0xff
		path := writeHandoffFile(t, root, "valid.png", tampered)

		_, err := promptInputWithPolicy(
			t.Context(),
			[]acp.ContentBlock{handoffBlock(path, imageMIMEPNG, data)},
			handoffPolicy(root, applyOptions(nil).ImageLimits),
		)
		requireHandoffError(t, err, 0, imageErrorDigestMismatch, handoffDigestMismatchMessage)
	})

	t.Run("declared size disagrees", func(t *testing.T) {
		root := t.TempDir()
		path := writeHandoffFile(t, root, "valid.png", data)

		_, err := promptInputWithPolicy(t.Context(), []acp.ContentBlock{
			handoffBlockWithEnvelope(path, imageMIMEPNG, map[string]any{
				handoffFieldVersion:   handoffVersion,
				handoffFieldDigest:    handoffDigest(data),
				handoffFieldSizeBytes: len(data) - 1,
			}),
		}, handoffPolicy(root, applyOptions(nil).ImageLimits))
		requireHandoffError(t, err, 0, imageErrorDigestMismatch, handoffSizeMismatchMessage)
	})
}

// TestHandoffBytesAreWithheldWhenVerificationFails pins the read function's own
// contract rather than the care its current caller takes with the result: bytes
// that failed the envelope check never leave handoffBytes, so a caller that
// forwards the data before it reads the failure has nothing to forward.
func TestHandoffBytesAreWithheldWhenVerificationFails(t *testing.T) {
	data := imageFixture(t, "valid.png")

	tampered := append([]byte(nil), data...)
	tampered[len(tampered)-1] ^= 0xff

	for _, test := range []struct {
		name    string
		stored  []byte
		message string
	}{
		{name: "bytes do not hash to the declared digest", stored: tampered, message: handoffDigestMismatchMessage},
		{name: "file is shorter than the declared size", stored: data[:len(data)-1], message: handoffSizeMismatchMessage},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			path := writeHandoffFile(t, root, "valid.png", test.stored)

			budget := imagePromptBudget{limits: applyOptions(nil).ImageLimits, handoffRoot: root}
			t.Cleanup(budget.closeHandoffRoot)

			block := handoffBlock(path, imageMIMEPNG, data)

			decoded, failure := budget.handoffBytes(t.Context(), block.Image)
			require.NotNil(t, failure)
			require.Equal(t, imageErrorDigestMismatch, failure.value)
			require.Equal(t, test.message, failure.message)
			require.Nil(t, decoded)
		})
	}
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
				t.Context(),
				[]acp.ContentBlock{handoffBlock(path, test.mimeType, data)},
				handoffPolicy(root, applyOptions(nil).ImageLimits),
			)
			requireInvalidParamsData(t, err, imageErrorData(0, test.want))
		})
	}

	t.Run("per image limit rejects the declared size", func(t *testing.T) {
		path := writeHandoffFile(t, root, "per-image.png", validPNG)
		limits := applyOptions(nil).ImageLimits
		limits.MaxInputBytesPerImage = int64(len(validPNG) - 1)

		_, err := promptInputWithPolicy(
			t.Context(),
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

		_, err := promptInputWithPolicy(t.Context(), []acp.ContentBlock{
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
			t.Context(),
			[]acp.ContentBlock{handoffBlock(path, imageMIMEPNG, wide)},
			handoffPolicy(root, applyOptions(nil).ImageLimits),
		)
		requireInvalidParamsData(t, err, imageErrorData(0, imageErrorNativeEnvelope))
	})

	t.Run("native envelope rejects a declared size no policy limit reaches", func(t *testing.T) {
		limits := ImageLimits{}
		declared := ampNativeMaxImageBytes + 1

		_, err := promptInputWithPolicy(t.Context(), []acp.ContentBlock{
			handoffBlockWithEnvelope(filepath.Join(root, "absent.png"), imageMIMEPNG, map[string]any{
				handoffFieldVersion:   handoffVersion,
				handoffFieldDigest:    handoffDigest(validPNG),
				handoffFieldSizeBytes: int(declared),
			}),
		}, handoffPolicy(root, limits))
		requireInvalidParamsData(t, err, imageSizeErrorData(
			0, imageErrorNativeEnvelope, declared, ampNativeMaxImageBytes,
		))
	})
}

// TestHandoffDeclaredMediaTypeIsJudgedBeforeTheFilesystem pins the pre-gate
// order: a declaration this adapter was never going to accept costs it no open,
// no read and no hash. The absent name proves the verdict needs no file at all;
// the name outside the root proves the declared type outranks the location, so a
// bad-MIME probe cannot learn whether the file it named is there.
func TestHandoffDeclaredMediaTypeIsJudgedBeforeTheFilesystem(t *testing.T) {
	root := t.TempDir()
	validPNG := imageFixture(t, "valid.png")
	outside := writeHandoffFile(t, t.TempDir(), "outside.png", validPNG)

	for _, test := range []struct {
		name string
		path string
	}{
		{name: "the name does not exist", path: filepath.Join(root, "absent.png")},
		{name: "the name leaves the root", path: outside},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := promptInputWithPolicy(t.Context(), []acp.ContentBlock{
				handoffBlock(test.path, "image/svg+xml", validPNG),
			}, handoffPolicy(root, applyOptions(nil).ImageLimits))
			requireInvalidParamsData(t, err, imageErrorData(0, imageErrorInvalidMediaType))
		})
	}
}

// TestHandoffOversizeReadIsRejectedWithoutForwardingBytes pins that no unverified
// byte survives the read: a size the caller itself declared past the gate is
// refused before anything is opened, and a file holding more than it was declared
// to hold contributes nothing to the native request.
func TestHandoffOversizeReadIsRejectedWithoutForwardingBytes(t *testing.T) {
	validPNG := imageFixture(t, "valid.png")
	bound := int64(len(validPNG))

	t.Run("a declared size past the gate is rejected before anything is opened", func(t *testing.T) {
		root := t.TempDir()
		outside := writeHandoffFile(t, t.TempDir(), "outside.png", validPNG)

		// No file is written inside the root and the second name would be refused
		// for its location, so the only thing that can produce a size verdict for
		// either is the caller's own declaration.
		for _, path := range []string{filepath.Join(root, "valid.png"), outside} {
			block := handoffBlockWithEnvelope(path, imageMIMEPNG, map[string]any{
				handoffFieldVersion:   handoffVersion,
				handoffFieldDigest:    handoffDigest(validPNG),
				handoffFieldSizeBytes: int(bound + 1),
			})

			_, err := promptInputWithPolicy(t.Context(), []acp.ContentBlock{block},
				handoffPolicy(root, ImageLimits{MaxInputBytesPerImage: bound}))
			requireInvalidParamsData(t, err, imageSizeErrorData(0, imageErrorTooLarge, bound+1, bound))
		}
	})

	t.Run("a file larger than its declaration forwards nothing", func(t *testing.T) {
		root := t.TempDir()

		// The file holds one byte more than the envelope describes, which is what
		// a file appended to after the block was written looks like. Those bytes
		// were never verified, so none of them may survive the read.
		grown := make([]byte, bound+1)
		copy(grown, validPNG)
		path := writeHandoffFile(t, root, "valid.png", grown)

		block := handoffBlockWithEnvelope(path, imageMIMEPNG, map[string]any{
			handoffFieldVersion:   handoffVersion,
			handoffFieldDigest:    handoffDigest(validPNG),
			handoffFieldSizeBytes: len(validPNG),
		})

		input, err := promptInputWithPolicy(t.Context(), []acp.ContentBlock{block},
			handoffPolicy(root, ImageLimits{MaxInputBytesPerImage: bound + 1}))
		requireHandoffError(t, err, 0, imageErrorDigestMismatch, handoffSizeMismatchMessage)
		require.Nil(t, input)
	})
}

// TestHandoffBlockCountCapRejectsWithAggregateDisabled pins the work bound: with
// the byte aggregate disabled and every block a small valid image, the block
// count is the only thing that can reject any of them.
func TestHandoffBlockCountCapRejectsWithAggregateDisabled(t *testing.T) {
	root := t.TempDir()
	data := imageFixture(t, "valid.png")
	path := writeHandoffFile(t, root, "valid.png", data)
	limits := ImageLimits{MaxInputBytesPerPrompt: 0}

	blocks := make([]acp.ContentBlock, 0, maxHandoffBlocksPerPrompt+1)
	for range maxHandoffBlocksPerPrompt + 1 {
		blocks = append(blocks, handoffBlock(path, imageMIMEPNG, data))
	}

	_, err := promptInputWithPolicy(t.Context(), blocks, handoffPolicy(root, limits))
	requireInvalidParamsData(t, err, imageSizeErrorData(
		maxHandoffBlocksPerPrompt,
		imageErrorTooLarge,
		maxHandoffBlocksPerPrompt+1,
		maxHandoffBlocksPerPrompt,
	))

	accepted, err := promptInputWithPolicy(t.Context(), blocks[:maxHandoffBlocksPerPrompt], handoffPolicy(root, limits))
	require.NoError(t, err)

	message, ok := accepted[keyMessage].(map[string]any)
	require.True(t, ok)
	content, ok := message[keyContent].([]map[string]any)
	require.True(t, ok)
	require.Len(t, content, maxHandoffBlocksPerPrompt)
}

// TestHandoffSymlinkContainmentIsKernelEnforced pins that a link beneath the root
// cannot name a location outside it, and that the verdict follows the error the
// open returns rather than any filesystem-shape check of the adapter's own.
func TestHandoffSymlinkContainmentIsKernelEnforced(t *testing.T) {
	data := imageFixture(t, "valid.png")

	outsideDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(outsideDir, "secret.png"), data, 0o600))

	for _, test := range []struct {
		name    string
		link    string
		target  string
		value   string
		message string
	}{
		{name: "a relative link inside the root resolves", link: "inside.png", target: "valid.png"},
		{
			name:    "a relative link out of the root is refused",
			link:    "escape.png",
			target:  filepath.Join("..", filepath.Base(outsideDir), "secret.png"),
			value:   imageErrorPathNotAllowed,
			message: handoffUnopenableMessage,
		},
		{
			name:    "an absolute link is refused even inside the root",
			link:    "absolute.png",
			value:   imageErrorPathNotAllowed,
			message: handoffUnopenableMessage,
		},
		{
			name:    "a link whose target was cleaned up is missing",
			link:    "dangling.png",
			target:  "gone.png",
			value:   imageErrorMissingFile,
			message: handoffFileAbsentMessage,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(root, "valid.png"), data, 0o600))

			target := test.target
			if target == "" {
				target = filepath.Join(root, "valid.png")
			}

			link := filepath.Join(root, test.link)
			require.NoError(t, os.Symlink(target, link))

			input, err := promptInputWithPolicy(
				t.Context(),
				[]acp.ContentBlock{handoffBlock(link, imageMIMEPNG, data)},
				handoffPolicy(root, applyOptions(nil).ImageLimits),
			)

			if test.value == "" {
				require.NoError(t, err)
				require.Equal(t, base64.StdEncoding.EncodeToString(data), promptContentBase64(t, input))

				return
			}

			requireHandoffError(t, err, 0, test.value, test.message)
		})
	}
}

// TestHandoffReadHonoursACancelledContext pins that the mapping phase, which runs
// before the turn state exists, still stops when its caller has gone away.
func TestHandoffReadHonoursACancelledContext(t *testing.T) {
	root := t.TempDir()
	data := imageFixture(t, "valid.png")
	path := writeHandoffFile(t, root, "valid.png", data)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := promptInputWithPolicy(
		ctx,
		[]acp.ContentBlock{handoffBlock(path, imageMIMEPNG, data)},
		handoffPolicy(root, applyOptions(nil).ImageLimits),
	)
	require.ErrorIs(t, err, context.Canceled)

	budget := imagePromptBudget{limits: applyOptions(nil).ImageLimits, handoffRoot: root}

	handle, failure := budget.handoffRootHandle()
	require.Nil(t, failure)

	t.Cleanup(budget.closeHandoffRoot)

	_, readFailure := readPromptHandoffBytes(ctx, handle, "valid.png", int64(len(data)))
	require.NotNil(t, readFailure)
	require.Equal(t, imageErrorMissingFile, readFailure.value)
	require.Equal(t, handoffUnreadableMessage, readFailure.message)
}

// rootSnapshot records every entry under root with the identity and size that
// would change if the adapter wrote, moved, or removed anything.
func rootSnapshot(t *testing.T, root string) map[string]string {
	t.Helper()

	snapshot := map[string]string{}

	require.NoError(t, filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		info, statErr := entry.Info()
		if statErr != nil {
			return statErr
		}

		modified := info.ModTime()
		if runtime.GOOS == "windows" && entry.IsDir() {
			// Windows publishes a directory's last-write time to its parent
			// entry lazily, so two stats taken either side of an operation that
			// touched neither can still disagree about when the directory last
			// changed. The entry set, the modes and the sizes still state
			// whether anything was written, moved or removed; only the
			// directory's own clock reading is dropped, and only where the
			// platform will not answer it consistently.
			modified = time.Time{}
		}

		snapshot[path] = fmt.Sprintf("%v|%d|%v", info.Mode(), info.Size(), modified)

		return nil
	}))

	return snapshot
}

// TestHandoffReadNeverMutatesTheRoot pins the read-only posture: a turn that
// consumed a file from the root leaves the tree exactly as it found it.
func TestHandoffReadNeverMutatesTheRoot(t *testing.T) {
	root := t.TempDir()
	data := imageFixture(t, "valid.png")
	path := writeHandoffFile(t, root, filepath.Join("session", "turn", "valid.png"), data)

	before := rootSnapshot(t, root)

	input, err := promptInputWithPolicy(
		t.Context(),
		[]acp.ContentBlock{handoffBlock(path, imageMIMEPNG, data)},
		handoffPolicy(root, applyOptions(nil).ImageLimits),
	)
	require.NoError(t, err)
	require.Equal(t, base64.StdEncoding.EncodeToString(data), promptContentBase64(t, input))

	require.Equal(t, before, rootSnapshot(t, root))
}

// TestHandoffMessagesCarryNoObservedValues drives a real failure at every stage
// of the pre-gate and pins that each message a client receives is one of this
// file's declared constants. A message added later that interpolated a path, a
// uri, a digest or a measured byte count would not be in the set, and would fail
// the disclosure assertions besides.
func TestHandoffMessagesCarryNoObservedValues(t *testing.T) {
	root := t.TempDir()
	data := imageFixture(t, "valid.png")
	path := writeHandoffFile(t, root, "valid.png", data)

	tampered := append([]byte(nil), data...)
	tampered[len(tampered)-1] ^= 0xff
	tamperedPath := writeHandoffFile(t, root, "tampered.png", tampered)

	outside := writeHandoffFile(t, t.TempDir(), "secret.png", data)

	escaping := filepath.Join(root, "escape.png")
	require.NoError(t, os.Symlink(outside, escaping))

	declared := map[string]bool{
		handoffRootUnsetMessage:            true,
		handoffRootUnopenableMessage:       true,
		handoffOutsideRootMessage:          true,
		handoffNotRegularMessage:           true,
		handoffFileAbsentMessage:           true,
		handoffUnopenableMessage:           true,
		handoffUninspectableMessage:        true,
		handoffUnreadableMessage:           true,
		handoffSizeMismatchMessage:         true,
		handoffDigestMismatchMessage:       true,
		handoffEnvelopeAbsentMessage:       true,
		handoffEnvelopeNotObjectMessage:    true,
		handoffEnvelopeUnknownFieldMessage: true,
		handoffVersionInvalidMessage:       true,
		handoffVersionUnsupportedMessage:   true,
		handoffDigestInvalidMessage:        true,
		handoffSizeBytesInvalidMessage:     true,
		handoffURIAbsentMessage:            true,
		handoffURIUnparsableMessage:        true,
		handoffURISchemeMessage:            true,
		handoffURIRemoteHostMessage:        true,
		handoffURIRelativeMessage:          true,
	}

	unparsable := acp.ImageBlock("", imageMIMEPNG)
	unparsable.Image.Uri = acp.Ptr("file://\x7f/x.png")
	unparsable.Image.Meta = map[string]any{metaHandoffKey: map[string]any{
		handoffFieldVersion:   handoffVersion,
		handoffFieldDigest:    handoffDigest(data),
		handoffFieldSizeBytes: len(data),
	}}

	envelopeWith := func(fields map[string]any) acp.ContentBlock {
		return handoffBlockWithEnvelope(path, imageMIMEPNG, fields)
	}

	for _, test := range []struct {
		name  string
		block acp.ContentBlock
		root  string
	}{
		{name: "no read root", block: handoffBlock(path, imageMIMEPNG, data)},
		{name: "unopenable root", block: handoffBlock(path, imageMIMEPNG, data), root: filepath.Join(root, "absent")},
		{name: "no envelope", block: handoffBlockWithEnvelope(path, imageMIMEPNG, nil), root: root},
		{name: "envelope is not an object", block: envelopeWith(nil), root: root},
		{
			name: "envelope carries an unknown field",
			block: envelopeWith(map[string]any{
				handoffFieldVersion: handoffVersion, handoffFieldDigest: handoffDigest(data),
				handoffFieldSizeBytes: len(data), "extra": true,
			}),
			root: root,
		},
		{
			name: "envelope version is unsupported",
			block: envelopeWith(map[string]any{
				handoffFieldVersion: 9, handoffFieldDigest: handoffDigest(data), handoffFieldSizeBytes: len(data),
			}),
			root: root,
		},
		{
			name: "envelope digest is malformed",
			block: envelopeWith(map[string]any{
				handoffFieldVersion: handoffVersion, handoffFieldDigest: "nope", handoffFieldSizeBytes: len(data),
			}),
			root: root,
		},
		{
			name: "envelope size is malformed",
			block: envelopeWith(map[string]any{
				handoffFieldVersion: handoffVersion, handoffFieldDigest: handoffDigest(data), handoffFieldSizeBytes: -3,
			}),
			root: root,
		},
		{name: "uri is unparsable", block: unparsable, root: root},
		{name: "path is outside the root", block: handoffBlock(outside, imageMIMEPNG, data), root: root},
		{name: "path escapes through a link", block: handoffBlock(escaping, imageMIMEPNG, data), root: root},
		{name: "path is a directory", block: handoffBlock(root, imageMIMEPNG, data), root: root},
		{name: "path is absent", block: handoffBlock(filepath.Join(root, "gone.png"), imageMIMEPNG, data), root: root},
		{name: "bytes are tampered", block: handoffBlock(tamperedPath, imageMIMEPNG, data), root: root},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := promptInputWithPolicy(
				t.Context(),
				[]acp.ContentBlock{test.block},
				handoffPolicy(test.root, applyOptions(nil).ImageLimits),
			)

			var reqErr *acp.RequestError

			require.ErrorAs(t, err, &reqErr)

			payload, ok := reqErr.Data.(map[string]any)
			require.True(t, ok)

			message, ok := payload[keyMessage].(string)
			require.True(t, ok)
			require.True(t, declared[message], "message is not a declared constant: %q", message)

			rendered := reqErr.Error()
			require.NotContains(t, rendered, root)
			require.NotContains(t, rendered, outside)
			require.NotContains(t, rendered, "valid.png")
			require.NotContains(t, rendered, "file://")
			require.NotRegexp(t, `[0-9a-f]{16}`, rendered)
		})
	}
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

// promptContentBase64 joins the base64 of every native image source in a built
// prompt.
func promptContentBase64(t *testing.T, input map[string]any) string {
	t.Helper()

	message, ok := input[keyMessage].(map[string]any)
	require.True(t, ok)
	content, ok := message[keyContent].([]map[string]any)
	require.True(t, ok)

	parts := make([]string, 0, len(content))

	for _, block := range content {
		source, sourceOK := block[keySource].(map[string]any)
		if !sourceOK {
			continue
		}

		encoded, encodedOK := source[keyData].(string)
		require.True(t, encodedOK)
		parts = append(parts, encoded)
	}

	return strings.Join(parts, "\n")
}
