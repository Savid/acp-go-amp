package ampacp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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

	// handoffParentDir is the path element a root-relative name may not start
	// with, which is how a lexical test tells a path that was never under the
	// read root from one that is.
	handoffParentDir = ".."

	// handoffSizeBytesExclusiveMax is 2^63 as a float64, the first value at or
	// above the int64 range. A declared size is validated against it as a float,
	// because converting an out-of-range float64 to int64 is undefined in Go and
	// wraps on one architecture while saturating on another.
	handoffSizeBytesExclusiveMax = 9223372036854775808.0

	// maxHandoffBlocksPerPrompt bounds the handoff-form blocks one prompt may ask
	// this adapter to read. A handoff block is a couple of hundred bytes on the
	// wire and drives a whole file read, so without a count bound one legal frame
	// commits the adapter to arbitrarily much I/O and resident memory whenever the
	// per-prompt byte aggregate is disabled. It bounds the work rather than the
	// byte policy, and sits far enough above any real multi-image turn that a
	// conforming host never meets it.
	maxHandoffBlocksPerPrompt = 64
)

// Handoff failure messages. Every one is a compile-time constant: a verdict
// travels to the client and into the trace backend, so it may never carry a
// path, a uri, a filename, a digest, a byte count of a file the caller did not
// describe, or operating-system error text.
const (
	handoffRootUnsetMessage      = "handoff input is not enabled: no handoff read root is configured"
	handoffRootUnopenableMessage = "handoff read root cannot be opened"
	handoffOutsideRootMessage    = "handoff path is outside the configured read root"
	handoffNotRegularMessage     = "handoff path is not a regular file"
	handoffFileAbsentMessage     = "handoff path does not exist"
	handoffUnopenableMessage     = "handoff file cannot be opened"
	handoffUninspectableMessage  = "handoff file cannot be inspected"
	handoffUnreadableMessage     = "handoff file cannot be read"
	handoffSizeMismatchMessage   = "handoff file does not hold the declared sizeBytes"
	handoffDigestMismatchMessage = "handoff file bytes do not hash to the declared digest"

	handoffEnvelopeAbsentMessage       = "image block carries no " + metaHandoffKey + " envelope"
	handoffEnvelopeNotObjectMessage    = metaHandoffKey + " envelope is not an object"
	handoffEnvelopeUnknownFieldMessage = metaHandoffKey + " envelope carries an unknown field"
	handoffVersionInvalidMessage       = metaHandoffKey + " envelope version is missing or not an integer"
	handoffVersionUnsupportedMessage   = metaHandoffKey + " envelope version is not supported"
	handoffDigestInvalidMessage        = metaHandoffKey + " envelope digest must be 64 lowercase hex characters"
	handoffSizeBytesInvalidMessage     = metaHandoffKey + " envelope sizeBytes is missing or not a non-negative integer"

	handoffURIAbsentMessage     = "handoff image block carries no uri"
	handoffURIUnparsableMessage = "handoff uri is not parseable"
	handoffURISchemeMessage     = "handoff uri scheme is not " + handoffURIScheme
	handoffURIRemoteHostMessage = "handoff uri names a remote host"
	handoffURIRelativeMessage   = "handoff uri path is not absolute"
)

// handoffFile is the part of an opened handoff file the bounded read needs. The
// mode is checked on the descriptor rather than on the path, because a read root
// bounds where a name may lead and never what kind of object it names.
type handoffFile interface {
	io.ReadCloser
	Stat() (os.FileInfo, error)
}

// openHandoffFile opens one name inside an already-opened read root. It is a
// seam so the inspection and read failures a real filesystem only produces under
// a race with the host that owns the file stay exercisable; containment is the
// root's, not this function's, on every path.
var openHandoffFile = func(root *os.Root, rel string) (handoffFile, error) {
	return root.OpenFile(rel, os.O_RDONLY|handoffOpenFlags, 0)
}

// handoffError is one handoff pre-gate failure: the image error value, the
// constant message naming the cause, and the byte pair a size verdict reports.
type handoffError struct {
	value     string
	message   string
	sizeBytes int64
	maxBytes  int64
}

func handoffInvalid(message string) *handoffError {
	return &handoffError{value: imageErrorInvalidHandoff, message: message}
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

// handoffBytes runs the handoff pre-gate for one image block: the per-prompt
// block count, envelope and uri strictness, the declared media type, the
// declared size against the per-image bounds, the name's position under the read
// root, a bounded root-relative read, then digest verification.
//
// Verdicts are ordered so a host can tell a malformed block (invalid_handoff)
// from a path it may not read (path_not_allowed), from a file the host already
// cleaned up (missing_file), from bytes that are not the ones it announced
// (handoff_digest_mismatch). Nothing the filesystem reports feeds a verdict, and
// every path that returns bytes has verified them against the envelope the
// caller sent.
func (b *imagePromptBudget) handoffBytes(ctx context.Context, block *acp.ContentBlockImage) ([]byte, *handoffError) {
	if b.handoffRoot == "" {
		return nil, handoffInvalid(handoffRootUnsetMessage)
	}

	b.handoffBlocks++
	if b.handoffBlocks > maxHandoffBlocksPerPrompt {
		return nil, &handoffError{
			value:     imageErrorTooLarge,
			sizeBytes: int64(b.handoffBlocks),
			maxBytes:  maxHandoffBlocksPerPrompt,
		}
	}

	envelope, failure := parsePromptHandoffEnvelope(block.Meta)
	if failure != nil {
		return nil, failure
	}

	requested, failure := promptHandoffRequestedPath(block.Uri)
	if failure != nil {
		return nil, failure
	}

	// The declaration is judged in full before the filesystem is consulted at all,
	// as the declared type is in the embedded form. A block this adapter was never
	// going to accept costs it no open, no read and no hash, and its refusal cannot
	// report whether the path it named exists.
	if !isPromptImageMIME(block.MimeType) {
		return nil, &handoffError{value: imageErrorInvalidMediaType}
	}

	// The size gate reads the caller's own declaration, so an oversize handoff is
	// rejected without measuring a file the caller may not be entitled to measure.
	if oversize := b.declaredSizeVerdict(envelope.sizeBytes); oversize != nil {
		return nil, oversize
	}

	rel, failure := promptHandoffRelativePath(b.handoffRoot, requested)
	if failure != nil {
		return nil, failure
	}

	root, failure := b.handoffRootHandle()
	if failure != nil {
		return nil, failure
	}

	data, failure := readPromptHandoffBytes(ctx, root, rel, envelope.sizeBytes)
	if failure != nil {
		return nil, failure
	}

	if failure := verifyPromptHandoffDigest(envelope, data); failure != nil {
		return nil, failure
	}

	return data, nil
}

// declaredSizeVerdict applies the per-image byte bounds to a declared size. The
// two bounds keep their own verdicts: a configured policy limit reports
// too_large, and Amp's unconditional native envelope reports
// native_envelope_exceeded, exactly as they do for embedded bytes.
func (b *imagePromptBudget) declaredSizeVerdict(sizeBytes int64) *handoffError {
	if maxBytes := b.limits.MaxInputBytesPerImage; maxBytes > 0 && sizeBytes > maxBytes {
		return &handoffError{value: imageErrorTooLarge, sizeBytes: sizeBytes, maxBytes: maxBytes}
	}

	if sizeBytes > ampNativeMaxImageBytes {
		return &handoffError{
			value:     imageErrorNativeEnvelope,
			sizeBytes: sizeBytes,
			maxBytes:  ampNativeMaxImageBytes,
		}
	}

	return nil
}

// handoffRootHandle opens the read root once per prompt. Containment is the
// kernel's from here on: every handoff open is relative to this descriptor and a
// symlink beneath it may not name a location outside it, so there is no window
// between deciding a path is inside the root and reading it. A root that cannot
// be opened is a deployment defect rather than a host cleaning a file up early,
// so it is path_not_allowed.
func (b *imagePromptBudget) handoffRootHandle() (*os.Root, *handoffError) {
	if b.root != nil {
		return b.root, nil
	}

	root, err := os.OpenRoot(b.handoffRoot)
	if err != nil {
		return nil, &handoffError{value: imageErrorPathNotAllowed, message: handoffRootUnopenableMessage}
	}

	b.root = root

	return root, nil
}

// closeHandoffRoot releases the read root's descriptor at the end of the prompt
// mapping that opened it.
func (b *imagePromptBudget) closeHandoffRoot() {
	if b.root != nil {
		_ = b.root.Close()
		b.root = nil
	}
}

// parsePromptHandoffEnvelope validates the block's handoff envelope. The
// envelope is strict: exactly three known fields, version 1, a 64-character
// lowercase hex digest, and a non-negative integer size.
func parsePromptHandoffEnvelope(meta map[string]any) (promptHandoffEnvelope, *handoffError) {
	raw, present := meta[metaHandoffKey]
	if !present {
		return promptHandoffEnvelope{}, handoffInvalid(handoffEnvelopeAbsentMessage)
	}

	fields, ok := raw.(map[string]any)
	if !ok {
		return promptHandoffEnvelope{}, handoffInvalid(handoffEnvelopeNotObjectMessage)
	}

	for name := range fields {
		switch name {
		case handoffFieldVersion, handoffFieldDigest, handoffFieldSizeBytes:
		default:
			return promptHandoffEnvelope{}, handoffInvalid(handoffEnvelopeUnknownFieldMessage)
		}
	}

	version, ok := promptHandoffNumber(fields[handoffFieldVersion])
	if !ok || version != math.Trunc(version) {
		return promptHandoffEnvelope{}, handoffInvalid(handoffVersionInvalidMessage)
	}

	if version != handoffVersion {
		return promptHandoffEnvelope{}, handoffInvalid(handoffVersionUnsupportedMessage)
	}

	digest, ok := fields[handoffFieldDigest].(string)
	if !ok || !isLowercaseHexDigest(digest) {
		return promptHandoffEnvelope{}, handoffInvalid(handoffDigestInvalidMessage)
	}

	sizeBytes, ok := promptHandoffSizeBytes(fields[handoffFieldSizeBytes])
	if !ok {
		return promptHandoffEnvelope{}, handoffInvalid(handoffSizeBytesInvalidMessage)
	}

	return promptHandoffEnvelope{digest: digest, sizeBytes: sizeBytes}, nil
}

// promptHandoffNumber reads an envelope numeric as a float64 whatever shape it
// arrived in. A JSON number decodes to float64 under the pinned SDK and to
// json.Number if the decoder is ever asked for one, and an in-process host
// builds its own block metadata with a Go int. All three are the same number,
// and reading them as a float64 is what keeps the range check off an int64
// conversion whose out-of-range behaviour Go does not define.
func promptHandoffNumber(value any) (float64, bool) {
	switch number := value.(type) {
	case float64:
		return number, true
	case json.Number:
		parsed, err := number.Float64()

		return parsed, err == nil
	case int:
		return float64(number), true
	default:
		return 0, false
	}
}

// promptHandoffSizeBytes validates a declared byte count entirely in float64,
// before any int64 conversion: non-negative, integral, and strictly below 2^63.
func promptHandoffSizeBytes(value any) (int64, bool) {
	size, ok := promptHandoffNumber(value)
	if !ok || size < 0 || size != math.Trunc(size) || size >= handoffSizeBytesExclusiveMax {
		return 0, false
	}

	return int64(size), true
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
func promptHandoffRequestedPath(uri *string) (string, *handoffError) {
	if uri == nil || *uri == "" {
		return "", handoffInvalid(handoffURIAbsentMessage)
	}

	parsed, err := url.Parse(*uri)
	if err != nil {
		return "", handoffInvalid(handoffURIUnparsableMessage)
	}

	if parsed.Scheme != handoffURIScheme {
		return "", handoffInvalid(handoffURISchemeMessage)
	}

	if parsed.Host != "" && parsed.Host != handoffURIHost {
		return "", handoffInvalid(handoffURIRemoteHostMessage)
	}

	path := filepath.FromSlash(parsed.Path)
	if !filepath.IsAbs(path) {
		return "", handoffInvalid(handoffURIRelativeMessage)
	}

	return filepath.Clean(path), nil
}

// promptHandoffRelativePath maps an absolute handoff path to the name it has
// inside the read root. The lexical test only decides which verdict to report
// for a path that was never under the root; the kernel, not this function, is
// what keeps a resolved name inside it.
func promptHandoffRelativePath(root, requested string) (string, *handoffError) {
	rel, err := filepath.Rel(filepath.Clean(root), requested)
	if err != nil || rel == handoffParentDir || strings.HasPrefix(rel, handoffParentDir+string(filepath.Separator)) {
		return "", &handoffError{value: imageErrorPathNotAllowed, message: handoffOutsideRootMessage}
	}

	return rel, nil
}

// readPromptHandoffBytes reads a handoff file by its name inside the read root,
// at most one byte more than the envelope declared.
//
// The size the read is bounded by is the caller's own declared sizeBytes,
// already checked against the per-image bounds, so a file that grew, shrank or
// was swapped after the block was written fails verification rather than passing
// a size gate on a stale number. The descriptor is opened without blocking and
// required to be a regular file, because a root bounds where a name may lead and
// not what kind of object it names.
func readPromptHandoffBytes(ctx context.Context, root *os.Root, rel string, declared int64) ([]byte, *handoffError) {
	if ctx.Err() != nil {
		return nil, &handoffError{value: imageErrorMissingFile, message: handoffUnreadableMessage}
	}

	file, err := openHandoffFile(root, rel)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, &handoffError{value: imageErrorMissingFile, message: handoffFileAbsentMessage}
		}

		return nil, &handoffError{value: imageErrorPathNotAllowed, message: handoffUnopenableMessage}
	}

	defer func() { _ = file.Close() }()

	info, err := file.Stat()
	if err != nil {
		return nil, &handoffError{value: imageErrorMissingFile, message: handoffUninspectableMessage}
	}

	if !info.Mode().IsRegular() {
		return nil, &handoffError{value: imageErrorPathNotAllowed, message: handoffNotRegularMessage}
	}

	data, err := io.ReadAll(io.LimitReader(file, declared+1))
	if err != nil {
		return nil, &handoffError{value: imageErrorMissingFile, message: handoffUnreadableMessage}
	}

	return data, nil
}

// verifyPromptHandoffDigest fails closed when the read bytes are not the ones
// the envelope described. Neither message reports what was observed: the caller
// already knows what it declared, and the file it named may not be one it is
// entitled to learn the size or the content hash of.
func verifyPromptHandoffDigest(envelope promptHandoffEnvelope, data []byte) *handoffError {
	if int64(len(data)) != envelope.sizeBytes {
		return &handoffError{value: imageErrorDigestMismatch, message: handoffSizeMismatchMessage}
	}

	if digest := sha256.Sum256(data); hex.EncodeToString(digest[:]) != envelope.digest {
		return &handoffError{value: imageErrorDigestMismatch, message: handoffDigestMismatchMessage}
	}

	return nil
}
