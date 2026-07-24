package ampacp

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-amp/internal/amp"
)

func TestToolImageOutputLiveReplayAndDiagnosticHygiene(t *testing.T) {
	ctx := context.Background()
	store := NewInMemorySessionStore()
	agent := newTestAgent(WithSessionStore(store))
	client, cleanup := attachRecordingClient(t, agent)
	defer cleanup()

	session := &agentSession{
		agent:     agent,
		id:        "T-image-output",
		nativeID:  "T-native-image-output",
		cwd:       t.TempDir(),
		rawEvents: true,
	}
	pngData := base64.StdEncoding.EncodeToString(imageFixture(t, "valid.png"))
	signedURL := "https://cdn.example/image.png?signature=secret"
	items := []any{
		map[string]any{"type": "text", "text": "generated"},
		map[string]any{
			"type": "image",
			"source": map[string]any{
				"type":       "base64",
				"media_type": imageMIMEPNG,
				"data":       pngData,
			},
		},
		map[string]any{
			"type":       "image",
			"mimeType":   imageMIMEPNG,
			"attachment": map[string]any{"type": "url", "url": signedURL},
		},
	}
	native := parseToolResultMessage(t, "TU-image", items, false)

	transcriptJSON, err := session.prepareMessageImageArtifacts(ctx, native)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(transcriptJSON, pngData) || strings.Contains(transcriptJSON, signedURL) {
		t.Fatal("sanitized transcript retained image data or signed URL")
	}

	if emitErr := session.emitRawEvent(ctx, "stream-json", native); emitErr != nil {
		t.Fatal(emitErr)
	}
	if emitErr := session.emitMessage(ctx, native, true, ""); emitErr != nil {
		t.Fatal(emitErr)
	}
	waitForRecorded(t, func() bool {
		return len(client.updatesSnapshot()) == 1 && len(client.rawSnapshot()) == 1
	})

	live := client.updatesSnapshot()[0].Update.ToolCallUpdate
	requireImageToolSnapshot(t, live, pngData, signedURL)
	encodedLive, err := json.Marshal(live.RawOutput)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encodedLive), signedURL) {
		t.Fatal("tool diagnostic retained signed URL")
	}
	rawBytes, _ := json.Marshal(client.rawSnapshot())
	if strings.Contains(string(rawBytes), pngData) || strings.Contains(string(rawBytes), signedURL) {
		t.Fatal("raw event retained image data or signed URL")
	}

	if persistErr := session.persistAfterTurn(ctx, []SessionStoreEntry{json.RawMessage(transcriptJSON)}); persistErr != nil {
		t.Fatal(persistErr)
	}
	subkeys, err := store.ListSubkeys(ctx, SessionKey{SessionID: string(session.id)})
	if err != nil {
		t.Fatal(err)
	}
	artifactCount := 0
	for _, subpath := range subkeys {
		if isImageArtifactSubpath(subpath) {
			artifactCount++
		}
	}
	if artifactCount != 2 {
		t.Fatalf("artifact subkeys = %d, want 2 (%v)", artifactCount, subkeys)
	}

	replayAgent := newTestAgent(WithSessionStore(store))
	replayClient, replayCleanup := attachRecordingClient(t, replayAgent)
	defer replayCleanup()
	replaySession := &agentSession{agent: replayAgent, id: session.id}
	replayed, err := amp.ParseJSONLine([]byte(transcriptJSON))
	if err != nil {
		t.Fatal(err)
	}
	if err := replaySession.emitMessage(ctx, replayed, false, ""); err != nil {
		t.Fatal(err)
	}
	waitForRecorded(t, func() bool { return len(replayClient.updatesSnapshot()) == 1 })
	replay := replayClient.updatesSnapshot()[0].Update.ToolCallUpdate

	liveJSON, _ := json.Marshal(live.Content)
	replayJSON, _ := json.Marshal(replay.Content)
	if string(liveJSON) != string(replayJSON) {
		t.Fatalf("live/replay content differs:\nlive %s\nreplay %s", liveJSON, replayJSON)
	}
}

func TestPainterStringifiedAttachmentBecomesResourceLink(t *testing.T) {
	items := []any{
		map[string]any{
			"type":       "image",
			"mimeType":   imageMIMEPNG,
			"attachment": map[string]any{"url": "https://cdn.example/painter.png?token=secret"},
		},
	}
	encoded, err := json.Marshal(items)
	if err != nil {
		t.Fatal(err)
	}

	agent := newTestAgent()
	client, cleanup := attachRecordingClient(t, agent)
	defer cleanup()
	session := &agentSession{agent: agent, id: "T-painter"}
	msg := parseToolResultMessage(t, "TU-painter", string(encoded), false)
	transcript, err := session.prepareMessageImageArtifacts(context.Background(), msg)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(transcript, "token=secret") {
		t.Fatal("transcript retained Painter signed URL")
	}
	if err := session.emitMessage(context.Background(), msg, true, ""); err != nil {
		t.Fatal(err)
	}
	waitForRecorded(t, func() bool { return len(client.updatesSnapshot()) == 1 })

	update := client.updatesSnapshot()[0].Update.ToolCallUpdate
	if len(update.Content) != 1 ||
		update.Content[0].Content == nil ||
		update.Content[0].Content.Content.ResourceLink == nil ||
		update.Content[0].Content.Content.ResourceLink.Uri != "https://cdn.example/painter.png?token=secret" {
		t.Fatalf("Painter content = %#v", update.Content)
	}
}

func TestOutputDoesNotApplyInputFormatAllowlist(t *testing.T) {
	bmp := make([]byte, 58)
	copy(bmp, "BM")
	binary.LittleEndian.PutUint32(bmp[2:6], uint32(len(bmp)))
	binary.LittleEndian.PutUint32(bmp[10:14], 54)
	binary.LittleEndian.PutUint32(bmp[14:18], 40)
	binary.LittleEndian.PutUint32(bmp[18:22], 1)
	binary.LittleEndian.PutUint32(bmp[22:26], 1)
	binary.LittleEndian.PutUint16(bmp[26:28], 1)
	binary.LittleEndian.PutUint16(bmp[28:30], 24)

	image, err := prepareNativeToolImage(map[string]any{
		"type":     "image",
		"mimeType": "image/bmp",
		"data":     base64.StdEncoding.EncodeToString(bmp),
	}, "TU-bmp", 0, applyOptions(nil).ImageLimits)
	if err != nil {
		t.Fatal(err)
	}
	if image.mimeType != "image/bmp" {
		t.Fatalf("MIME = %q", image.mimeType)
	}
}

func TestImageOutputFailuresUseTransportEnvelope(t *testing.T) {
	validPNG := imageFixture(t, "valid.png")
	validData := base64.StdEncoding.EncodeToString(validPNG)
	for _, test := range []struct {
		name   string
		raw    map[string]any
		limits ImageLimits
		reason string
	}{
		{
			name:   "invalid base64",
			raw:    map[string]any{"type": "image", "mimeType": imageMIMEPNG, "data": "%%%"},
			limits: applyOptions(nil).ImageLimits,
			reason: imageOutputInvalidBase64,
		},
		{
			name:   "not raster",
			raw:    map[string]any{"type": "image", "data": base64.StdEncoding.EncodeToString([]byte("not an image"))},
			limits: applyOptions(nil).ImageLimits,
			reason: imageOutputNotRaster,
		},
		{
			name:   "MIME mismatch",
			raw:    map[string]any{"type": "image", "mimeType": imageMIMEJPEG, "data": validData},
			limits: applyOptions(nil).ImageLimits,
			reason: imageOutputMediaTypeMismatch,
		},
		{
			name:   "missing representation",
			raw:    map[string]any{"type": "image", "mimeType": imageMIMEPNG},
			limits: applyOptions(nil).ImageLimits,
			reason: imageOutputMissingFile,
		},
		{
			name:   "local path rejected",
			raw:    map[string]any{"type": "image", "uri": "file:///tmp/image.png"},
			limits: applyOptions(nil).ImageLimits,
			reason: imageOutputPathNotAllowed,
		},
		{
			name: "per image limit",
			raw:  map[string]any{"type": "image", "mimeType": imageMIMEPNG, "data": validData},
			limits: ImageLimits{
				MaxOutputBytesPerImage:    int64(len(validPNG) - 1),
				MaxOutputBytesPerToolCall: defaultImageLimitBytes,
			},
			reason: imageOutputTooLarge,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := prepareNativeToolImage(test.raw, "TU", 0, test.limits)
			requireImageOutputFailure(t, err, test.reason)
		})
	}
}

func TestImageOutputStructuralDefectsPrecedeTooLarge(t *testing.T) {
	validPNG := imageFixture(t, "valid.png")
	oversizeLimits := ImageLimits{
		MaxOutputBytesPerImage:    int64(len(validPNG) - 1),
		MaxOutputBytesPerToolCall: defaultImageLimitBytes,
	}

	notRaster := base64.StdEncoding.EncodeToString(append([]byte("not an image"), make([]byte, len(validPNG))...))
	_, err := prepareNativeToolImage(
		map[string]any{"type": "image", "mimeType": imageMIMEPNG, "data": notRaster},
		"TU-oversize-nonraster", 0, oversizeLimits,
	)
	requireImageOutputFailure(t, err, imageOutputNotRaster)

	_, err = prepareNativeToolImage(
		map[string]any{"type": "image", "mimeType": imageMIMEJPEG, "data": base64.StdEncoding.EncodeToString(validPNG)},
		"TU-oversize-mismatch", 0, oversizeLimits,
	)
	requireImageOutputFailure(t, err, imageOutputMediaTypeMismatch)
}

func TestImageOutputMIMEDeclarationIgnoresUnrelatedNestedFields(t *testing.T) {
	pngData := base64.StdEncoding.EncodeToString(imageFixture(t, "valid.png"))
	raw := map[string]any{
		"type":        "image",
		"annotations": map[string]any{"media_type": "application/json"},
		"source":      map[string]any{"type": "base64", "data": pngData},
	}

	if got := imageMIMEField(raw); got != "" {
		t.Fatalf("imageMIMEField bound an unrelated nested media type: %q", got)
	}

	image, err := prepareNativeToolImage(raw, "TU-scoped-mime", 0, applyOptions(nil).ImageLimits)
	if err != nil || image.mimeType != imageMIMEPNG {
		t.Fatalf("scoped MIME image = (%#v, %v)", image, err)
	}
}

func TestImageOutputToolAggregateAndFailureLifecycle(t *testing.T) {
	validPNG := imageFixture(t, "valid.png")
	validData := base64.StdEncoding.EncodeToString(validPNG)
	limits := applyOptions(nil).ImageLimits
	limits.MaxOutputBytesPerToolCall = int64(len(validPNG)*2 - 1)
	agent := newTestAgent(WithImageLimits(limits))
	client, cleanup := attachRecordingClient(t, agent)
	defer cleanup()
	session := &agentSession{agent: agent, id: "T-output-limit"}

	msg := parseToolResultMessage(t, "TU-limit", []any{
		map[string]any{"type": "image", "mimeType": imageMIMEPNG, "data": validData},
		map[string]any{"type": "image", "mimeType": imageMIMEPNG, "data": validData},
	}, false)
	_, err := session.prepareMessageImageArtifacts(context.Background(), msg)
	requireImageOutputFailure(t, err, imageOutputTooLarge)
	waitForRecorded(t, func() bool { return len(client.updatesSnapshot()) == 1 })

	update := client.updatesSnapshot()[0].Update.ToolCallUpdate
	if update == nil || update.Status == nil || *update.Status != acp.ToolCallStatusFailed {
		t.Fatalf("mapping failure lifecycle = %#v", update)
	}
}

func TestPromptFailsOnNativeImageMappingError(t *testing.T) {
	path, _ := fakeAgentAmpPath(t, "image-output-error")
	agent := newTestAgent(
		WithExecutablePath(path),
		WithScratchDir(t.TempDir()),
	)
	client, cleanup := attachRecordingClient(t, agent)
	defer cleanup()

	session, err := agent.NewSession(context.Background(), NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}

	_, err = agent.Prompt(
		context.Background(),
		TextPromptRequest(session.SessionId, "turn-image-error", "generate an image"),
	)
	requireImageOutputFailure(t, err, imageOutputInvalidBase64)
	waitForRecorded(t, func() bool { return len(client.updatesSnapshot()) > 0 })

	updates := client.updatesSnapshot()
	failed := updates[len(updates)-1].Update.ToolCallUpdate
	if failed == nil || failed.Status == nil || *failed.Status != acp.ToolCallStatusFailed {
		t.Fatalf("image mapping failure update = %#v", failed)
	}
}

func TestNativeFailedToolStillCarriesMappedImageContent(t *testing.T) {
	agent := newTestAgent()
	client, cleanup := attachRecordingClient(t, agent)
	defer cleanup()
	session := &agentSession{agent: agent, id: "T-native-failed"}
	msg := parseToolResultMessage(t, "TU-native-failed", []any{
		map[string]any{
			"type":       "image",
			"mimeType":   imageMIMEPNG,
			"attachment": map[string]any{"url": "https://cdn.example/failed.png"},
		},
	}, true)
	if _, err := session.prepareMessageImageArtifacts(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if err := session.emitMessage(context.Background(), msg, true, ""); err != nil {
		t.Fatal(err)
	}
	waitForRecorded(t, func() bool { return len(client.updatesSnapshot()) == 1 })

	update := client.updatesSnapshot()[0].Update.ToolCallUpdate
	if update.Status == nil || *update.Status != acp.ToolCallStatusFailed || len(update.Content) != 1 {
		t.Fatalf("native failed content = %#v", update)
	}
}

func TestExpiredImageArtifactFailsReplayAndIsSwept(t *testing.T) {
	originalNow := imageArtifactNow
	t.Cleanup(func() { imageArtifactNow = originalNow })
	now := time.Unix(1_800_000_000, 0)
	imageArtifactNow = func() time.Time { return now }

	store := NewInMemorySessionStore()
	agent := newTestAgent(WithSessionStore(store))
	session := &agentSession{agent: agent, id: "T-expired"}
	msg := parseToolResultMessage(t, "TU-expired", []any{
		map[string]any{
			"type":       "image",
			"mimeType":   imageMIMEPNG,
			"attachment": map[string]any{"url": "https://cdn.example/expired.png"},
		},
	}, false)
	transcript, err := session.prepareMessageImageArtifacts(context.Background(), msg)
	if err != nil {
		t.Fatal(err)
	}
	session.nativeID = "T-native-expired"
	if persistErr := session.persistAfterTurn(context.Background(), []SessionStoreEntry{json.RawMessage(transcript)}); persistErr != nil {
		t.Fatal(persistErr)
	}

	user, ok := msg.(*amp.UserMessage)
	if !ok {
		t.Fatalf("message = %T", msg)
	}
	result, ok := user.Content[0].(amp.ToolResultBlock)
	if !ok {
		t.Fatalf("content = %#v", user.Content)
	}
	items, ok := result.Content.([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("tool content = %#v", result.Content)
	}
	refItem, ok := items[0].(map[string]any)
	if !ok {
		t.Fatalf("artifact item = %#v", items[0])
	}
	ref, ok := refItem["ref"].(string)
	if !ok {
		t.Fatalf("artifact ref = %#v", refItem["ref"])
	}
	now = now.Add(imageArtifactTTL + time.Millisecond)
	if sweepErr := agent.sweepExpiredImageArtifacts(context.Background()); sweepErr != nil {
		t.Fatal(sweepErr)
	}

	_, _, err = session.toolResultSnapshot(
		context.Background(),
		result,
	)
	requireImageOutputFailure(t, err, imageOutputStorageFailed)
	entries, loadErr := store.Load(context.Background(), SessionKey{
		SessionID: string(session.id),
		Subpath:   ref,
	})
	if loadErr != nil || len(entries) != 0 {
		t.Fatalf("expired artifact not swept: entries=%d err=%v", len(entries), loadErr)
	}
}

func parseToolResultMessage(t *testing.T, toolUseID string, content any, isError bool) amp.Message {
	t.Helper()
	line, err := json.Marshal(map[string]any{
		"type": "user",
		"message": map[string]any{
			"content": []any{map[string]any{
				"type":        "tool_result",
				"tool_use_id": toolUseID,
				"content":     content,
				"is_error":    isError,
			}},
		},
		"session_id": "T-native",
	})
	if err != nil {
		t.Fatal(err)
	}
	msg, err := amp.ParseJSONLine(line)
	if err != nil {
		t.Fatal(err)
	}

	return msg
}

func requireImageToolSnapshot(
	t *testing.T,
	update *acp.SessionToolCallUpdate,
	pngData string,
	signedURL string,
) {
	t.Helper()
	if update == nil || update.Status == nil || *update.Status != acp.ToolCallStatusCompleted {
		t.Fatalf("tool update = %#v", update)
	}
	if len(update.Content) != 3 {
		t.Fatalf("tool content len = %d", len(update.Content))
	}
	if update.Content[0].Content == nil ||
		update.Content[0].Content.Content.Text == nil ||
		update.Content[0].Content.Content.Text.Text != "generated" {
		t.Fatalf("text content = %#v", update.Content[0])
	}
	if update.Content[1].Content == nil ||
		update.Content[1].Content.Content.Image == nil ||
		update.Content[1].Content.Content.Image.Data != pngData {
		t.Fatalf("image content = %#v", update.Content[1])
	}
	if update.Content[2].Content == nil ||
		update.Content[2].Content.Content.ResourceLink == nil ||
		update.Content[2].Content.Content.ResourceLink.Uri != signedURL {
		t.Fatalf("resource-link content = %#v", update.Content[2])
	}
}

func requireImageOutputFailure(t *testing.T, err error, reason string) {
	t.Helper()
	var requestErr *acp.RequestError
	if !errors.As(err, &requestErr) {
		t.Fatalf("error = %T %v, want RequestError", err, err)
	}
	if requestErr.Code != -32603 {
		t.Fatalf("code = %d, want -32603", requestErr.Code)
	}
	data, ok := requestErr.Data.(map[string]any)
	if !ok ||
		data[jsonFieldError] != turnFailedError ||
		data["cause"] != causeTransport ||
		data["stage"] != imageOutputStage ||
		data["reason"] != reason {
		t.Fatalf("image output failure data = %#v", requestErr.Data)
	}
}

func TestStructuredToolResultShapes(t *testing.T) {
	for _, test := range []struct {
		value any
		ok    bool
		len   int
	}{
		{value: []any{"one"}, ok: true, len: 1},
		{value: map[string]any{"type": "text"}, ok: true, len: 1},
		{value: `[{"type":"text"}]`, ok: true, len: 1},
		{value: `{"type":"text"}`, ok: true, len: 1},
		{value: `{`, ok: false},
		{value: `1`, ok: false},
		{value: 1, ok: false},
	} {
		items, ok := structuredToolResultItems(test.value)
		if ok != test.ok || len(items) != test.len {
			t.Fatalf("structured result %#v = (%#v, %t)", test.value, items, ok)
		}
	}
}

func TestPrepareNativeToolImageRepresentationEdges(t *testing.T) {
	png := imageFixture(t, "valid.png")
	pngData := base64.StdEncoding.EncodeToString(png)
	pngURL := "data:image/png;base64," + pngData

	for _, test := range []struct {
		name   string
		raw    map[string]any
		reason string
	}{
		{
			name:   "invalid data URL",
			raw:    map[string]any{"type": "image", "data": "data:image/png,not-base64"},
			reason: imageOutputInvalidBase64,
		},
		{
			name: "data URL MIME mismatch",
			raw: map[string]any{
				"type":     "image",
				"mimeType": imageMIMEJPEG,
				"data":     pngURL,
			},
			reason: imageOutputMediaTypeMismatch,
		},
		{
			name: "invalid location data URL",
			raw: map[string]any{
				"type":       "image",
				"attachment": map[string]any{"url": "data:image/png,not-base64"},
			},
			reason: imageOutputInvalidBase64,
		},
		{
			name: "location data URL MIME mismatch",
			raw: map[string]any{
				"type":       "image",
				"mimeType":   imageMIMEJPEG,
				"attachment": map[string]any{"url": pngURL},
			},
			reason: imageOutputMediaTypeMismatch,
		},
		{
			name:   "malformed remote URL",
			raw:    map[string]any{"type": "image", "url": "https://%"},
			reason: imageOutputPathNotAllowed,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := prepareNativeToolImage(test.raw, "TU-edge", 0, applyOptions(nil).ImageLimits)
			requireImageOutputFailure(t, err, test.reason)
		})
	}

	for _, raw := range []map[string]any{
		{"type": "image", "data": pngURL},
		{"type": "image", "attachment": map[string]any{"url": pngURL}},
		{
			"type": "image",
			"source": map[string]any{
				"type": "base64",
				"data": pngData,
			},
		},
	} {
		image, err := prepareNativeToolImage(raw, "TU-valid", 0, ImageLimits{})
		if err != nil || image.mimeType != imageMIMEPNG || string(image.data) != string(png) {
			t.Fatalf("native image %#v = (%#v, %v)", raw, image, err)
		}
	}

	if effectiveImageOutputLimit(0) != maxACPImageDecodedBytes ||
		effectiveImageOutputLimit(maxACPImageDecodedBytes+1) != maxACPImageDecodedBytes ||
		effectiveImageOutputLimit(1) != 1 {
		t.Fatal("effective output image limit is wrong")
	}

	for _, uri := range []string{"", "file:///tmp/image.png", "https://", "https://%"} {
		if isRemoteImageURI(uri) {
			t.Fatalf("non-remote image URI accepted: %q", uri)
		}
	}
	if !isRemoteImageURI("http://cdn.example/image.png") {
		t.Fatal("remote image URI rejected")
	}
}

func TestImageOutputHelperEdges(t *testing.T) {
	if mime, data, ok := splitImageDataURL("data:image/png;base64,AAAA"); !ok ||
		mime != imageMIMEPNG || data != "AAAA" {
		t.Fatalf("data URL = (%q, %q, %t)", mime, data, ok)
	}
	for _, value := range []string{"", "data:image/png;base64", "data:image/png,AAAA", "text/plain;base64,AAAA"} {
		if _, _, ok := splitImageDataURL(value); ok {
			t.Fatalf("invalid data URL accepted: %q", value)
		}
	}

	nested := map[string]any{
		"z": []any{
			7,
			map[string]any{"target": "found"},
		},
	}
	if got := recursiveStringField(nested, "target"); got != "found" {
		t.Fatalf("recursive map field = %q", got)
	}
	if got := recursiveStringField(nested["z"], "target"); got != "found" {
		t.Fatalf("recursive slice field = %q", got)
	}
	if got := recursiveStringField(7, "target"); got != "" {
		t.Fatalf("recursive scalar field = %q", got)
	}

	if _, _, _, ok := inspectOutputRaster([]byte("not an image")); ok {
		t.Fatal("non-raster output accepted")
	}
	if _, _, _, ok := inspectOutputRaster(pngImageSignature); ok {
		t.Fatal("header-only PNG output accepted")
	}

	bmp := make([]byte, 26)
	copy(bmp, "BM")
	binary.LittleEndian.PutUint32(bmp[18:22], ^uint32(0))
	binary.LittleEndian.PutUint32(bmp[22:26], 1)
	if _, _, _, ok := inspectOutputRaster(bmp); ok {
		t.Fatal("oversize BMP width accepted")
	}

	binary.LittleEndian.PutUint32(bmp[18:22], 1)
	binary.LittleEndian.PutUint32(bmp[22:26], ^uint32(0))
	if mime, width, height, ok := inspectOutputRaster(bmp); !ok ||
		mime != imageMIMEBMP || width != 1 || height != 1 {
		t.Fatalf("top-down BMP = (%q, %d, %d, %t)", mime, width, height, ok)
	}
}

func TestImageOutputCanonicalizationPreservesDiagnostics(t *testing.T) {
	ctx := context.Background()
	pngData := base64.StdEncoding.EncodeToString(imageFixture(t, "valid.png"))
	session := &agentSession{
		agent: newTestAgent(),
		id:    "T-canonical-diagnostics",
	}
	msg := parseToolResultMessage(t, "TU-canonical", []any{
		nil,
		map[string]any{"type": "custom", "value": "kept"},
		map[string]any{"type": "image", "data": pngData},
	}, false)

	transcript, err := session.prepareMessageImageArtifacts(ctx, msg)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(transcript, `"type":"custom"`) {
		t.Fatalf("custom diagnostic dropped: %s", transcript)
	}

	user, ok := msg.(*amp.UserMessage)
	if !ok {
		t.Fatalf("message = %T", msg)
	}
	result, ok := user.Content[0].(amp.ToolResultBlock)
	if !ok {
		t.Fatalf("tool result = %#v", user.Content)
	}
	items, ok := result.Content.([]any)
	if !ok {
		t.Fatalf("tool content = %#v", result.Content)
	}
	if len(items) != 3 || items[0] != nil {
		t.Fatalf("canonical content = %#v", items)
	}
	ref, ok := items[2].(map[string]any)
	if !ok || ref[keyType] != canonicalImageArtifactType {
		t.Fatalf("image position lost: %#v", items)
	}

	content, diagnostic, err := session.toolResultSnapshot(ctx, result)
	if err != nil || len(content) != 1 {
		t.Fatalf("tool snapshot = (%#v, %#v, %v)", content, diagnostic, err)
	}

	textOnly := amp.ToolResultBlock{ToolUseID: "TU-text", Content: []any{
		map[string]any{"type": "text", "text": "only"},
	}}
	if canonical, changed, err := session.prepareToolResultArtifacts(ctx, textOnly); err != nil ||
		changed || canonical != nil {
		t.Fatalf("text-only canonicalization = (%#v, %t, %v)", canonical, changed, err)
	}

	if _, _, err := session.toolResultSnapshot(ctx, amp.ToolResultBlock{Content: "plain"}); err != nil {
		t.Fatalf("plain tool result snapshot: %v", err)
	}
}

func TestPrepareMessageImageArtifactFailures(t *testing.T) {
	ctx := context.Background()
	pngData := base64.StdEncoding.EncodeToString(imageFixture(t, "valid.png"))

	result, err := amp.ParseJSONLine([]byte(`{"type":"result","subtype":"success"}`))
	if err != nil {
		t.Fatal(err)
	}
	if got, nonUserErr := (&agentSession{agent: newTestAgent()}).
		prepareMessageImageArtifacts(ctx, result); nonUserErr != nil ||
		got != result.RawJSON() {
		t.Fatalf("non-user transcript = (%q, %v)", got, nonUserErr)
	}

	appendFailure := &imageStoreTestDouble{
		InMemorySessionStore: NewInMemorySessionStore(),
		appendErr:            errors.New("append failed"),
	}
	failedSession := testArtifactSession(appendFailure, "T-output-store-failure")
	failedMessage := parseToolResultMessage(t, "TU-store-failure", []any{
		map[string]any{"type": "image", "data": pngData},
	}, false)
	if _, storeErr := failedSession.prepareMessageImageArtifacts(ctx, failedMessage); storeErr == nil {
		t.Fatal("image artifact store failure ignored")
	}

	if _, _, imageErr := failedSession.prepareToolResultArtifacts(ctx, amp.ToolResultBlock{
		ToolUseID: "TU-invalid-image",
		Content: []any{
			map[string]any{"type": "image", "data": "%%%"},
		},
	}); imageErr == nil {
		t.Fatal("invalid native image accepted during canonicalization")
	}

	marshalSession := &agentSession{agent: newTestAgent(), id: "T-output-marshal-failure"}
	marshalMessage := parseToolResultMessage(t, "TU-marshal-failure", []any{
		map[string]any{"type": "image", "data": pngData},
	}, false)
	marshalUser, ok := marshalMessage.(*amp.UserMessage)
	if !ok {
		t.Fatalf("message = %T", marshalMessage)
	}
	marshalUser.Raw["unsupported"] = make(chan struct{})
	_, err = marshalSession.prepareMessageImageArtifacts(ctx, marshalMessage)
	requireImageOutputFailure(t, err, imageOutputStorageFailed)
}

func TestToolResultSnapshotStorageEdges(t *testing.T) {
	ctx := context.Background()
	png := imageFixture(t, "valid.png")
	pngData := base64.StdEncoding.EncodeToString(png)

	store := NewInMemorySessionStore()
	session := testArtifactSession(store, "T-snapshot")

	if _, _, err := session.toolResultSnapshot(ctx, artifactResult("bad-ref", "identity")); err == nil {
		t.Fatal("invalid artifact reference accepted")
	}
	if err := session.emitMessage(ctx, &amp.UserMessage{Content: []amp.ContentBlock{
		amp.ToolResultBlock{
			ToolUseID: "TU-invalid-ref",
			Content:   []any{artifactItem("bad-ref", "identity")},
		},
	}}, true, ""); err == nil {
		t.Fatal("invalid artifact replay emitted successfully")
	}

	valid := storedImageArtifact{
		Version:     imageArtifactVersion,
		Kind:        imageArtifactKindEmbedded,
		Identity:    "tool:valid:0",
		Fingerprint: fingerprintImageOutput(png),
		MimeType:    imageMIMEPNG,
		Data:        pngData,
		CreatedAt:   time.Now().UnixMilli(),
	}
	validRef := appendSnapshotArtifact(t, store, session.id, valid)
	if _, _, err := session.toolResultSnapshot(ctx, artifactResult(validRef, "wrong")); err == nil {
		t.Fatal("artifact identity mismatch accepted")
	}

	invalidBase64 := valid
	invalidBase64.Identity = "tool:invalid-base64:0"
	invalidBase64.Fingerprint = "invalid-base64"
	invalidBase64.Data = "%%%"
	invalidRef := appendSnapshotArtifact(t, store, session.id, invalidBase64)
	if _, _, err := session.toolResultSnapshot(
		ctx,
		artifactResult(invalidRef, invalidBase64.Identity),
	); err == nil {
		t.Fatal("invalid stored base64 accepted")
	}

	mismatch := valid
	mismatch.Identity = "tool:mismatch:0"
	mismatch.MimeType = imageMIMEJPEG
	mismatchRef := appendSnapshotArtifact(t, store, session.id, mismatch)
	if _, _, err := session.toolResultSnapshot(ctx, artifactResult(mismatchRef, mismatch.Identity)); err == nil {
		t.Fatal("stored image metadata mismatch accepted")
	}

	smallLimits := applyOptions(nil).ImageLimits
	smallLimits.MaxOutputBytesPerImage = int64(len(png) - 1)
	smallSession := &agentSession{
		agent: newTestAgent(WithSessionStore(store), WithImageLimits(smallLimits)),
		id:    session.id,
	}
	if _, _, err := smallSession.toolResultSnapshot(ctx, artifactResult(validRef, valid.Identity)); err == nil {
		t.Fatal("stored image over current limit accepted")
	}

	second := valid
	second.Identity = "tool:valid:1"
	secondRef := appendSnapshotArtifact(t, store, session.id, second)
	aggregateLimits := applyOptions(nil).ImageLimits
	aggregateLimits.MaxOutputBytesPerToolCall = int64(len(png)*2 - 1)
	aggregateSession := &agentSession{
		agent: newTestAgent(WithSessionStore(store), WithImageLimits(aggregateLimits)),
		id:    session.id,
	}
	aggregate := amp.ToolResultBlock{Content: []any{
		artifactItem(validRef, valid.Identity),
		artifactItem(secondRef, second.Identity),
	}}
	if _, _, err := aggregateSession.toolResultSnapshot(ctx, aggregate); err == nil {
		t.Fatal("stored tool image aggregate over current limit accepted")
	}
}

func TestSanitizeImageDiagnosticEdges(t *testing.T) {
	value := []any{
		map[string]any{
			"type":     "image",
			"mimeType": imageMIMEPNG,
			"data":     "secret",
		},
		map[string]any{
			"type": "wrapper",
			"nested": map[string]any{
				"type": "image",
				"url":  "https://secret.example/image.png",
			},
		},
		`{"type":"image","data":"secret"}`,
		"plain",
		7,
	}
	encoded, err := json.Marshal(sanitizeImageDiagnostic(value))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "secret") {
		t.Fatalf("diagnostic retained image secret: %s", encoded)
	}
	if !strings.Contains(string(encoded), "plain") {
		t.Fatalf("diagnostic dropped plain value: %s", encoded)
	}
}

func TestEmitImageToolFailureNativeStatusAndDeliveryError(t *testing.T) {
	want := errors.New("delivery failed")
	client := &failingImageUpdateClient{
		recordingClient: &recordingClient{},
		err:             want,
	}
	agent := newTestAgent()
	agent.setConnection(client)
	session := &agentSession{agent: agent, id: "T-failed-delivery"}

	err := session.emitImageToolFailure(
		context.Background(),
		"TU-failed",
		true,
		"",
		errors.New("mapping failed"),
	)
	if !errors.Is(err, want) {
		t.Fatalf("image failure delivery = %v", err)
	}
}

type failingImageUpdateClient struct {
	*recordingClient
	err error
}

func (*failingImageUpdateClient) Done() <-chan struct{} {
	return make(chan struct{})
}

func (c *failingImageUpdateClient) SessionUpdate(context.Context, acp.SessionNotification) error {
	return c.err
}

func (*failingImageUpdateClient) NotifyExtension(context.Context, string, any) error {
	return nil
}

func artifactResult(ref, identity string) amp.ToolResultBlock {
	return amp.ToolResultBlock{Content: []any{artifactItem(ref, identity)}}
}

func artifactItem(ref, identity string) map[string]any {
	return map[string]any{
		keyType:    canonicalImageArtifactType,
		"ref":      ref,
		"identity": identity,
	}
}

func appendSnapshotArtifact(
	t *testing.T,
	store *InMemorySessionStore,
	sessionID acp.SessionId,
	artifact storedImageArtifact,
) string {
	t.Helper()

	ref := imageArtifactSubpath(artifact.Identity, artifact.Fingerprint)
	entry, _ := json.Marshal(artifact)
	if err := store.Append(context.Background(), SessionKey{
		SessionID: string(sessionID),
		Subpath:   ref,
	}, []SessionStoreEntry{entry}); err != nil {
		t.Fatal(err)
	}

	return ref
}
