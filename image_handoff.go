package ampacp

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/coder/acp-go-sdk"
)

const (
	// metaHandoffKey is the family-reserved _meta key for the local handoff input
	// form. It is a host-set key on an inbound image content block and an agent
	// capability advertisement at initialize; both use this one literal.
	metaHandoffKey = "acp-go.dev/handoff"
	// handoffVersion is the only accepted handoff envelope version.
	handoffVersion = 1

	handoffFieldVersion   = "version"
	handoffFieldDigest    = "digest"
	handoffFieldSizeBytes = "sizeBytes"

	// handoffDigestLength is the character length of the lowercase hex sha256 the
	// envelope must carry.
	handoffDigestLength = 64

	handoffURIScheme = "file"
	handoffURIHost   = "localhost"
)

// handoffFile is the part of an opened handoff file the bounded read needs. The
// mode and identity are re-checked on the descriptor rather than only on the
// path, so a file swapped between resolution and read is rejected instead of
// read.
type handoffFile interface {
	io.ReadCloser
	Stat() (os.FileInfo, error)
}

// These are seams so every resolution and read failure branch is exercisable;
// production always uses the standard library.
var (
	evalSymlinks    = filepath.EvalSymlinks
	statHandoffFile = os.Stat
	openHandoffFile = func(name string) (handoffFile, error) {
		return os.OpenFile(name, os.O_RDONLY|handoffOpenFlags, 0)
	}
)

// handoffCause renders an operating-system failure without the path it names, so
// a verdict returned to a client cannot disclose host filesystem layout while
// still naming the real cause.
func handoffCause(err error) string {
	var pathErr *fs.PathError
	if errors.As(err, &pathErr) {
		return pathErr.Err.Error()
	}

	return err.Error()
}

// promptHandoffEnvelope is a validated _meta["acp-go.dev/handoff"] payload.
type promptHandoffEnvelope struct {
	digest    string
	sizeBytes int64
}

// promptHandoffIntent reports whether an empty-data image block is claiming the
// handoff form: it carries the handoff envelope key, or its uri names a local
// file. A block with neither signal never draws a handoff verdict.
func promptHandoffIntent(block *acp.ContentBlockImage) bool {
	if _, present := block.Meta[metaHandoffKey]; present {
		return true
	}

	if block.Uri == nil {
		return false
	}

	parsed, err := url.Parse(*block.Uri)

	return err == nil && parsed.Scheme == handoffURIScheme
}

// readPromptHandoffImage resolves, reads and verifies the handoff-form image
// block at index. Verdicts are ordered so a host can tell a malformed block
// (invalid_handoff) from a path it may not read (path_not_allowed), from a file
// the host already cleaned up (missing_file), from bytes that are not the ones
// it announced (handoff_digest_mismatch).
//
// The read is bounded at maxBytes+1, the tightest per-image gate plus one. When
// the whole file fits, its size and digest are verified against the envelope and
// nothing unverified proceeds. When it does not fit the digest is unverifiable,
// so the returned size is the file's real size and the caller's size gates
// reject it — the bytes are never forwarded either way.
func readPromptHandoffImage(index int, root string, block *acp.ContentBlockImage, maxBytes int64) ([]byte, int64, error) {
	if root == "" {
		return nil, 0, imagePromptHandoffError(index, imageErrorInvalidHandoff,
			"handoff input is not enabled: no handoff read root is configured")
	}

	envelope, err := parsePromptHandoffEnvelope(block.Meta)
	if err != nil {
		return nil, 0, imagePromptHandoffError(index, imageErrorInvalidHandoff, err.Error())
	}

	requested, err := promptHandoffRequestedPath(block.Uri)
	if err != nil {
		return nil, 0, imagePromptHandoffError(index, imageErrorInvalidHandoff, err.Error())
	}

	resolved, info, verdict, err := resolvePromptHandoffPath(root, requested)
	if err != nil {
		return nil, 0, imagePromptHandoffError(index, verdict, err.Error())
	}

	data, sizeBytes, verdict, err := readPromptHandoffBytes(resolved, info, maxBytes)
	if err != nil {
		return nil, 0, imagePromptHandoffError(index, verdict, err.Error())
	}

	if sizeBytes > maxBytes {
		return data, sizeBytes, nil
	}

	if sizeBytes != envelope.sizeBytes {
		return nil, 0, imagePromptHandoffError(index, imageErrorDigestMismatch,
			fmt.Sprintf("handoff file holds %d bytes, envelope declares %d", sizeBytes, envelope.sizeBytes))
	}

	if digest := sha256.Sum256(data); hex.EncodeToString(digest[:]) != envelope.digest {
		return nil, 0, imagePromptHandoffError(index, imageErrorDigestMismatch,
			"handoff file bytes do not hash to the declared digest")
	}

	return data, sizeBytes, nil
}

// parsePromptHandoffEnvelope validates the block's handoff envelope. The
// envelope is strict: exactly three known fields, version 1, a 64-character
// lowercase hex digest, and a non-negative integer size.
func parsePromptHandoffEnvelope(meta map[string]any) (promptHandoffEnvelope, error) {
	raw, present := meta[metaHandoffKey]
	if !present {
		return promptHandoffEnvelope{}, fmt.Errorf("image block carries no %s envelope", metaHandoffKey)
	}

	fields, ok := raw.(map[string]any)
	if !ok {
		return promptHandoffEnvelope{}, fmt.Errorf("%s envelope is not an object", metaHandoffKey)
	}

	for name := range fields {
		switch name {
		case handoffFieldVersion, handoffFieldDigest, handoffFieldSizeBytes:
		default:
			return promptHandoffEnvelope{}, fmt.Errorf("%s envelope carries unknown field %q", metaHandoffKey, name)
		}
	}

	version, err := promptHandoffInt(fields, handoffFieldVersion)
	if err != nil {
		return promptHandoffEnvelope{}, err
	}

	if version != handoffVersion {
		return promptHandoffEnvelope{}, fmt.Errorf("%s envelope version %d is not supported", metaHandoffKey, version)
	}

	digest, ok := fields[handoffFieldDigest].(string)
	if !ok || !isLowercaseHexDigest(digest) {
		return promptHandoffEnvelope{}, fmt.Errorf(
			"%s envelope digest must be %d lowercase hex characters", metaHandoffKey, handoffDigestLength)
	}

	sizeBytes, err := promptHandoffInt(fields, handoffFieldSizeBytes)
	if err != nil {
		return promptHandoffEnvelope{}, err
	}

	if sizeBytes < 0 {
		return promptHandoffEnvelope{}, fmt.Errorf("%s envelope sizeBytes is negative", metaHandoffKey)
	}

	return promptHandoffEnvelope{digest: digest, sizeBytes: sizeBytes}, nil
}

// promptHandoffInt reads an integral envelope field. A JSON number arrives as a
// float64 over the transport and as a Go integer from an embedding host, so both
// are read; a fractional or non-numeric value is a defect.
func promptHandoffInt(fields map[string]any, name string) (int64, error) {
	switch value := fields[name].(type) {
	case float64:
		// math.MaxInt64 is not representable as a float64 and rounds up to 2^63, so
		// the upper comparison has to exclude the boundary itself.
		if value != math.Trunc(value) || value < math.MinInt64 || value >= math.MaxInt64 {
			return 0, fmt.Errorf("%s envelope %s is not an integer", metaHandoffKey, name)
		}

		return int64(value), nil
	case int:
		return int64(value), nil
	case int64:
		return value, nil
	default:
		return 0, fmt.Errorf("%s envelope %s is missing or not an integer", metaHandoffKey, name)
	}
}

func isLowercaseHexDigest(digest string) bool {
	if len(digest) != handoffDigestLength {
		return false
	}

	for _, char := range digest {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}

	return true
}

// promptHandoffRequestedPath extracts the local path a handoff block names. Only
// a file URI with no host (or localhost) and an absolute path is a request the
// adapter will resolve; everything else is malformed as a block.
func promptHandoffRequestedPath(uri *string) (string, error) {
	if uri == nil || *uri == "" {
		return "", errors.New("handoff image block carries no uri")
	}

	parsed, err := url.Parse(*uri)
	if err != nil {
		return "", fmt.Errorf("handoff uri is not parseable: %v", err)
	}

	if parsed.Scheme != handoffURIScheme {
		return "", fmt.Errorf("handoff uri scheme %q is not %q", parsed.Scheme, handoffURIScheme)
	}

	if parsed.Host != "" && parsed.Host != handoffURIHost {
		return "", fmt.Errorf("handoff uri names remote host %q", parsed.Host)
	}

	path := filepath.FromSlash(parsed.Path)
	if !filepath.IsAbs(path) {
		return "", fmt.Errorf("handoff uri path %q is not absolute", parsed.Path)
	}

	return path, nil
}

// resolvePromptHandoffPath resolves a requested path to a regular file inside the
// read root, returning the resolved path and the mode it was inspected with.
// Containment is checked lexically on the cleaned path, symlinks are then
// resolved, and containment is re-checked on the resolved path, so neither the
// request nor any link along it can leave the root. Failure causes never name a
// host path.
func resolvePromptHandoffPath(root, requested string) (string, os.FileInfo, string, error) {
	resolvedRoot, err := evalSymlinks(filepath.Clean(root))
	if err != nil {
		return "", nil, imageErrorPathNotAllowed,
			fmt.Errorf("handoff read root cannot be resolved: %s", handoffCause(err))
	}

	cleaned := filepath.Clean(requested)
	if !pathWithinRoot(filepath.Clean(root), cleaned) {
		return "", nil, imageErrorPathNotAllowed, errors.New("handoff path is outside the configured read root")
	}

	resolved, err := evalSymlinks(cleaned)
	if err != nil {
		if isNotExistError(err) {
			return "", nil, imageErrorMissingFile, fmt.Errorf("handoff path does not exist: %s", handoffCause(err))
		}

		return "", nil, imageErrorPathNotAllowed, fmt.Errorf("handoff path cannot be resolved: %s", handoffCause(err))
	}

	if !pathWithinRoot(resolvedRoot, resolved) {
		return "", nil, imageErrorPathNotAllowed, errors.New("handoff path resolves outside the configured read root")
	}

	info, err := statHandoffFile(resolved)
	if err != nil {
		if isNotExistError(err) {
			return "", nil, imageErrorMissingFile, fmt.Errorf("handoff path does not exist: %s", handoffCause(err))
		}

		return "", nil, imageErrorPathNotAllowed, fmt.Errorf("handoff path cannot be inspected: %s", handoffCause(err))
	}

	if !info.Mode().IsRegular() {
		return "", nil, imageErrorPathNotAllowed, errors.New("handoff path is not a regular file")
	}

	return resolved, info, "", nil
}

// pathWithinRoot reports whether an already-cleaned path is the root or sits
// beneath it.
func pathWithinRoot(root, path string) bool {
	if path == root {
		return true
	}

	return strings.HasPrefix(path, root+string(os.PathSeparator))
}

func isNotExistError(err error) bool {
	return errors.Is(err, fs.ErrNotExist)
}

// readPromptHandoffBytes reads at most limit+1 bytes so the caller can tell a
// payload that fits its gate from one that does not, without holding an unbounded
// file in memory. The returned size is the number of bytes read, or the file's
// real size when the file exceeded the bound.
//
// The descriptor is verified against the inspection that admitted the path: the
// bytes read must come from that same regular file, so a path swapped for a
// symlink, a FIFO, or another file between resolution and read is rejected rather
// than read.
func readPromptHandoffBytes(path string, resolved os.FileInfo, limit int64) ([]byte, int64, string, error) {
	file, err := openHandoffFile(path)
	if err != nil {
		return nil, 0, imageErrorMissingFile, fmt.Errorf("handoff file cannot be opened: %s", handoffCause(err))
	}

	defer func() { _ = file.Close() }()

	opened, err := file.Stat()
	if err != nil {
		return nil, 0, imageErrorMissingFile, fmt.Errorf("handoff file cannot be inspected: %s", handoffCause(err))
	}

	if !opened.Mode().IsRegular() || !os.SameFile(resolved, opened) {
		return nil, 0, imageErrorPathNotAllowed,
			errors.New("handoff file is not the regular file that was resolved")
	}

	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, 0, imageErrorMissingFile, fmt.Errorf("handoff file cannot be read: %s", handoffCause(err))
	}

	sizeBytes := int64(len(data))
	if sizeBytes <= limit {
		return data, sizeBytes, "", nil
	}

	// The bound stopped the read, so the file is larger than every size the gates
	// admit. Report the real size when the descriptor still agrees it is larger,
	// and never a size the gates would accept.
	if opened.Size() > sizeBytes {
		sizeBytes = opened.Size()
	}

	return data, sizeBytes, "", nil
}
