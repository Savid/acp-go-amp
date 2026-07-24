package ampacp

import (
	"encoding/base64"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/coder/acp-go-sdk"
)

func TestImagePromptValidationAcceptsPortableFormats(t *testing.T) {
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
			data := imageFixture(t, test.name)
			block := acp.ImageBlock(base64.StdEncoding.EncodeToString(data), test.mimeType)
			block.Image.Uri = acp.Ptr("https://invalid.example/ignored")

			input, err := promptInput([]acp.ContentBlock{block})
			if err != nil {
				t.Fatalf("prompt input: %v", err)
			}
			message, ok := input[keyMessage].(map[string]any)
			if !ok {
				t.Fatalf("message = %#v", input[keyMessage])
			}
			content, ok := message[keyContent].([]map[string]any)
			if !ok || len(content) != 1 {
				t.Fatalf("content = %#v", message[keyContent])
			}
			source, ok := content[0][keySource].(map[string]any)
			if !ok {
				t.Fatalf("source = %#v", content[0][keySource])
			}
			if source["data"] != base64.StdEncoding.EncodeToString(data) {
				t.Fatal("validated bytes changed")
			}
		})
	}
}

func TestImagePromptValidationErrors(t *testing.T) {
	validPNG := imageFixture(t, "valid.png")
	validPNGData := base64.StdEncoding.EncodeToString(validPNG)

	for _, test := range []struct {
		name string
		data string
		mime string
		want map[string]any
	}{
		{
			name: "missing data",
			mime: imageMIMEPNG,
			want: imageErrorData(0, imageErrorMissingData),
		},
		{
			name: "invalid media type before decoding",
			data: "%%%",
			mime: "IMAGE/PNG",
			want: imageErrorData(0, imageErrorInvalidMediaType),
		},
		{
			name: "invalid base64",
			data: "%%%",
			mime: imageMIMEPNG,
			want: imageErrorData(0, imageErrorInvalidBase64),
		},
		{
			name: "media type mismatch",
			data: base64.StdEncoding.EncodeToString(imageFixture(t, "mismatch.png")),
			mime: imageMIMEPNG,
			want: imageErrorData(0, imageErrorMediaTypeMismatch),
		},
		{
			name: "truncated dimensions",
			data: base64.StdEncoding.EncodeToString(imageFixture(t, "truncated.png")),
			mime: imageMIMEPNG,
			want: imageErrorData(0, imageErrorInvalidDimensions),
		},
		{
			name: "animated gif",
			data: base64.StdEncoding.EncodeToString(imageFixture(t, "animated.gif")),
			mime: imageMIMEGIF,
			want: imageErrorData(0, imageErrorAnimatedNotSupported),
		},
		{
			name: "animated webp",
			data: base64.StdEncoding.EncodeToString(imageFixture(t, "animated.webp")),
			mime: imageMIMEWebP,
			want: imageErrorData(0, imageErrorAnimatedNotSupported),
		},
		{
			name: "animated png",
			data: base64.StdEncoding.EncodeToString(imageFixture(t, "animated-apng.png")),
			mime: imageMIMEPNG,
			want: imageErrorData(0, imageErrorAnimatedNotSupported),
		},
		{
			name: "actl is animated even with one declared frame",
			data: base64.StdEncoding.EncodeToString(imageFixture(t, "single-frame-actl.png")),
			mime: imageMIMEPNG,
			want: imageErrorData(0, imageErrorAnimatedNotSupported),
		},
		{
			name: "per image limit",
			data: validPNGData,
			mime: imageMIMEPNG,
			want: imageSizeErrorData(0, imageErrorTooLarge, int64(len(validPNG)), int64(len(validPNG)-1)),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			limits := applyOptions(nil).ImageLimits
			if test.name == "per image limit" {
				limits.MaxInputBytesPerImage = int64(len(validPNG) - 1)
			}
			_, err := promptInputWithLimits(
				[]acp.ContentBlock{acp.ImageBlock(test.data, test.mime)},
				limits,
			)
			requireInvalidParamsData(t, err, test.want)
		})
	}
}

func TestImagePromptAggregateLimitUsesCrossingImageIndex(t *testing.T) {
	validPNG := imageFixture(t, "valid.png")
	encoded := base64.StdEncoding.EncodeToString(validPNG)
	limits := applyOptions(nil).ImageLimits
	limits.MaxInputBytesPerPrompt = int64(len(validPNG)*2 - 1)

	_, err := promptInputWithLimits([]acp.ContentBlock{
		acp.TextBlock("first"),
		acp.ImageBlock(encoded, imageMIMEPNG),
		acp.TextBlock("second"),
		acp.ImageBlock(encoded, imageMIMEPNG),
	}, limits)
	requireInvalidParamsData(t, err, imageSizeErrorData(
		1,
		imageErrorTooLarge,
		int64(len(validPNG)*2),
		limits.MaxInputBytesPerPrompt,
	))
}

func TestImagePromptNativeEnvelopes(t *testing.T) {
	validPNG := imageFixture(t, "valid.png")
	oversize := make([]byte, ampNativeMaxImageBytes+1)
	copy(oversize, validPNG)

	limits := applyOptions(nil).ImageLimits
	limits.MaxInputBytesPerImage = 0
	limits.MaxInputBytesPerPrompt = 0
	_, err := promptInputWithLimits([]acp.ContentBlock{
		acp.ImageBlock(base64.StdEncoding.EncodeToString(oversize), imageMIMEPNG),
	}, limits)
	requireInvalidParamsData(t, err, imageSizeErrorData(
		0,
		imageErrorNativeEnvelope,
		int64(len(oversize)),
		ampNativeMaxImageBytes,
	))

	wide := append([]byte(nil), validPNG...)
	binary.BigEndian.PutUint32(wide[16:20], ampNativeMaxImageDimension+1)
	_, err = promptInputWithLimits([]acp.ContentBlock{
		acp.ImageBlock(base64.StdEncoding.EncodeToString(wide), imageMIMEPNG),
	}, applyOptions(nil).ImageLimits)
	requireInvalidParamsData(t, err, imageErrorData(0, imageErrorNativeEnvelope))
}

func TestImagePromptStructuralErrorsPrecedeNativeEnvelope(t *testing.T) {
	limits := applyOptions(nil).ImageLimits
	limits.MaxInputBytesPerImage = 0
	limits.MaxInputBytesPerPrompt = 0

	mismatch := make([]byte, ampNativeMaxImageBytes+1)

	badHeader := make([]byte, ampNativeMaxImageBytes+1)
	copy(badHeader, pngImageSignature)

	for _, test := range []struct {
		name string
		data []byte
		want string
	}{
		{name: "oversize bytes fail sniff before envelope", data: mismatch, want: imageErrorMediaTypeMismatch},
		{name: "oversize bytes fail structure before envelope", data: badHeader, want: imageErrorInvalidDimensions},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := promptInputWithLimits([]acp.ContentBlock{
				acp.ImageBlock(base64.StdEncoding.EncodeToString(test.data), imageMIMEPNG),
			}, limits)
			requireInvalidParamsData(t, err, imageErrorData(0, test.want))
		})
	}
}

func TestImagePromptPerImageTooLargeYieldsToStructuralDefects(t *testing.T) {
	limits := applyOptions(nil).ImageLimits
	limits.MaxInputBytesPerImage = ampNativeMaxImageBytes
	limits.MaxInputBytesPerPrompt = 0

	oversize := func(seed []byte) []byte {
		buf := make([]byte, ampNativeMaxImageBytes+1)
		copy(buf, seed)

		return buf
	}

	for _, test := range []struct {
		name string
		data []byte
		mime string
		want string
	}{
		{
			name: "unsniffable bytes report mismatch",
			data: oversize(nil),
			mime: imageMIMEPNG,
			want: imageErrorMediaTypeMismatch,
		},
		{
			name: "malformed header reports dimensions",
			data: oversize(pngImageSignature),
			mime: imageMIMEPNG,
			want: imageErrorInvalidDimensions,
		},
		{
			name: "animated frames report animation",
			data: oversize(imageFixture(t, "animated.gif")),
			mime: imageMIMEGIF,
			want: imageErrorAnimatedNotSupported,
		},
		{
			name: "declared type mismatch reports mismatch",
			data: oversize(imageFixture(t, "valid.png")),
			mime: imageMIMEJPEG,
			want: imageErrorMediaTypeMismatch,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := promptInputWithLimits([]acp.ContentBlock{
				acp.ImageBlock(base64.StdEncoding.EncodeToString(test.data), test.mime),
			}, limits)
			requireInvalidParamsData(t, err, imageErrorData(0, test.want))
		})
	}
}

func TestImagePromptAnimationBeyondRetainWindowPrecedesSizeVerdict(t *testing.T) {
	// The second frame descriptor sits beyond the amp native size window but
	// within the ACP transport frame cap. Structural inspection must still see
	// it so animation is reported instead of a size verdict.
	gif := animatedGIFWithSecondFrameBeyond(t, int(ampNativeMaxImageBytes)+200_000)
	if int64(len(gif)) <= ampNativeMaxImageBytes || int64(len(gif)) > maxACPImageDecodedBytes {
		t.Fatalf("fixture size %d outside (nativeMax, frameCap] window", len(gif))
	}

	if _, _, animated, err := inspectPromptImage(imageMIMEGIF, gif); err != nil || !animated {
		t.Fatalf("full-byte inspection = (animated %t, %v)", animated, err)
	}
	if _, _, animated, err := inspectPromptImage(imageMIMEGIF, gif[:ampNativeMaxImageBytes+1]); err != nil ||
		animated {
		t.Fatalf("old-window inspection = (animated %t, %v)", animated, err)
	}

	_, err := promptInputWithLimits(
		[]acp.ContentBlock{acp.ImageBlock(base64.StdEncoding.EncodeToString(gif), imageMIMEGIF)},
		applyOptions(nil).ImageLimits,
	)
	requireInvalidParamsData(t, err, imageErrorData(0, imageErrorAnimatedNotSupported))
}

func animatedGIFWithSecondFrameBeyond(t *testing.T, beyond int) []byte {
	t.Helper()

	buf := []byte("GIF89a")
	buf = append(buf, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00) // 1x1, no global color table
	buf = append(buf, 0x2c, 0, 0, 0, 0, 0x01, 0x00, 0x01, 0x00, 0x00)
	buf = append(buf, 0x08) // LZW minimum code size

	for len(buf) < beyond {
		block := make([]byte, 256)
		block[0] = 0xff

		buf = append(buf, block...)
	}

	buf = append(buf, 0x00)                                           // image-data terminator
	buf = append(buf, 0x2c, 0, 0, 0, 0, 0x01, 0x00, 0x01, 0x00, 0x00) // second frame
	buf = append(buf, 0x08, 0x00, 0x3b)

	return buf
}

func TestEmbeddedImageResourceUsesPromptImageBudget(t *testing.T) {
	validPNG := imageFixture(t, "valid.png")
	encoded := base64.StdEncoding.EncodeToString(validPNG)
	limits := applyOptions(nil).ImageLimits
	limits.MaxInputBytesPerPrompt = int64(len(validPNG)*2 - 1)

	_, err := promptInputWithLimits([]acp.ContentBlock{
		acp.ImageBlock(encoded, imageMIMEPNG),
		acp.ResourceBlock(acp.EmbeddedResourceResource{
			BlobResourceContents: &acp.BlobResourceContents{
				Blob:     encoded,
				MimeType: acp.Ptr(imageMIMEPNG),
				Uri:      "file:///ignored.png",
			},
		}),
	}, limits)
	requireInvalidParamsData(t, err, imageSizeErrorData(
		1,
		imageErrorTooLarge,
		int64(len(validPNG)*2),
		limits.MaxInputBytesPerPrompt,
	))
}

func imageFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "image", name))
	if err != nil {
		t.Fatal(err)
	}

	return data
}

func imageErrorData(index int, errorValue string) map[string]any {
	return map[string]any{
		jsonFieldField: imageField,
		jsonFieldError: errorValue,
		"index":        index,
	}
}

func imageSizeErrorData(index int, errorValue string, sizeBytes, maxBytes int64) map[string]any {
	data := imageErrorData(index, errorValue)
	data["sizeBytes"] = sizeBytes
	data["maxBytes"] = maxBytes

	return data
}

func TestImageInputStructuralEdges(t *testing.T) {
	writer := &boundedImageWriter{limit: 2}
	if written, err := writer.Write([]byte("four")); err != nil || written != 4 ||
		string(writer.data) != "fo" || writer.size != 4 {
		t.Fatalf("bounded writer = (%d, %v, %q, %d)", written, err, writer.data, writer.size)
	}

	if _, _, _, err := inspectPromptImage("image/unknown", nil); err == nil {
		t.Fatal("unknown image format accepted")
	}

	garbage := base64.StdEncoding.EncodeToString([]byte("not an image"))
	_, err := promptInputWithLimits(
		[]acp.ContentBlock{acp.ImageBlock(garbage, imageMIMEPNG)},
		applyOptions(nil).ImageLimits,
	)
	requireInvalidParamsData(t, err, imageErrorData(0, imageErrorMediaTypeMismatch))

	testPNGStructuralEdges(t)
	testJPEGStructuralEdges(t)
	testGIFStructuralEdges(t)
	testWebPStructuralEdges(t)
}

func testPNGStructuralEdges(t *testing.T) {
	t.Helper()

	valid := imageFixture(t, "valid.png")
	badLength := append([]byte(nil), valid...)
	binary.BigEndian.PutUint32(badLength[8:12], 12)
	if _, _, _, err := inspectPromptPNG(badLength); err == nil {
		t.Fatal("PNG with non-IHDR length accepted")
	}

	zeroWidth := append([]byte(nil), valid...)
	binary.BigEndian.PutUint32(zeroWidth[16:20], 0)
	if _, _, _, err := inspectPromptPNG(zeroWidth); err == nil {
		t.Fatal("PNG with zero width accepted")
	}

	header := append([]byte(nil), valid[:33]...)
	truncatedChunk := append(header, 0, 0, 0, 10, 't', 'E', 'S', 'T')
	width, height, animated, err := inspectPromptPNG(truncatedChunk)
	if err != nil || width == 0 || height == 0 || animated {
		t.Fatalf("PNG truncated after dimensions = (%d, %d, %t, %v)", width, height, animated, err)
	}

	unknownThenIDAT := append(header, 0, 0, 0, 0, 't', 'E', 'S', 'T', 0, 0, 0, 0)
	unknownThenIDAT = append(unknownThenIDAT, 0, 0, 0, 0, 'I', 'D', 'A', 'T')
	width, height, animated, err = inspectPromptPNG(unknownThenIDAT)
	if err != nil || width == 0 || height == 0 || animated {
		t.Fatalf("PNG chunk walk = (%d, %d, %t, %v)", width, height, animated, err)
	}

	width, height, animated, err = inspectPromptPNG(header)
	if err != nil || width == 0 || height == 0 || animated {
		t.Fatalf("PNG header residual = (%d, %d, %t, %v)", width, height, animated, err)
	}
}

func testJPEGStructuralEdges(t *testing.T) {
	t.Helper()

	cases := [][]byte{
		{0xff, 0xd8, 0xff},
		{0xff, 0xd8, 0xff, 0x00},
		{0xff, 0xd8, 0xff, 0xd8},
		{0xff, 0xd8, 0xff, 0xd9},
		{0xff, 0xd8, 0xff, 0xe0, 0x00},
		{0xff, 0xd8, 0xff, 0xe0, 0x00, 0x01},
		{0xff, 0xd8, 0xff, 0xe0, 0x00, 0x10},
		{0xff, 0xd8, 0xff, 0xc0, 0x00, 0x06, 0, 0, 0, 0},
		{0xff, 0xd8, 0xff, 0xc0, 0x00, 0x07, 8, 0, 0, 0, 1},
		{0xff, 0xd8, 0xff, 0xe0, 0x00, 0x02, 0xff, 0xd9},
	}
	for _, data := range cases {
		if _, _, err := inspectPromptJPEG(data); err == nil {
			t.Fatalf("malformed JPEG accepted: %x", data)
		}
	}

	if !isJPEGFrameMarker(0xc0) || isJPEGFrameMarker(0xc4) || isJPEGFrameMarker(0xd0) {
		t.Fatal("JPEG frame marker classification is wrong")
	}
}

func testGIFStructuralEdges(t *testing.T) {
	t.Helper()

	if _, _, _, err := inspectPromptGIF(make([]byte, 12)); err == nil {
		t.Fatal("short GIF accepted")
	}

	header := []byte("GIF89a")
	header = append(header, 0, 0, 1, 0, 0, 0, 0)
	if _, _, _, err := inspectPromptGIF(header); err == nil {
		t.Fatal("zero-width GIF accepted")
	}

	header[6] = 1
	for _, suffix := range [][]byte{
		{0x21},
		{0x21, 0xf9, 0},
		{0x3b},
		{0x7f},
	} {
		data := append(append([]byte(nil), header...), suffix...)
		width, height, animated, err := inspectPromptGIF(data)
		if err != nil || width != 1 || height != 1 || animated {
			t.Fatalf("GIF edge %x = (%d, %d, %t, %v)", suffix, width, height, animated, err)
		}
	}

	if got := skipPromptGIFImage([]byte{0x2c}, 0); got != 1 {
		t.Fatalf("short GIF image skip = %d", got)
	}

	localTable := make([]byte, 11)
	localTable[9] = 0x80
	if got := skipPromptGIFImage(localTable, 0); got != len(localTable) {
		t.Fatalf("GIF local table skip = %d", got)
	}

	noData := make([]byte, 10)
	if got := skipPromptGIFImage(noData, 0); got != len(noData) {
		t.Fatalf("GIF empty image skip = %d", got)
	}

	if got := skipPromptGIFSubBlocks([]byte{3, 1}, 0); got != 2 {
		t.Fatalf("truncated GIF sub-block skip = %d", got)
	}
	if got := skipPromptGIFSubBlocks([]byte{1, 7}, 0); got != 2 {
		t.Fatalf("terminal GIF sub-block skip = %d", got)
	}
}

func testWebPStructuralEdges(t *testing.T) {
	t.Helper()

	if _, _, _, err := inspectPromptWebP(make([]byte, 19)); err == nil {
		t.Fatal("short WebP accepted")
	}

	webp := func(kind string, payload []byte) []byte {
		data := make([]byte, 20+len(payload))
		copy(data[:4], "RIFF")
		copy(data[8:12], "WEBP")
		copy(data[12:16], kind)
		copy(data[20:], payload)

		return data
	}

	for _, data := range [][]byte{
		webp("VP8X", nil),
		webp("VP8 ", make([]byte, 10)),
		webp("VP8L", nil),
		webp("VP8L", []byte{0, 0, 0, 0, 0}),
		webp("NOPE", make([]byte, 10)),
	} {
		if _, _, _, err := inspectPromptWebP(data); err == nil {
			t.Fatalf("invalid WebP accepted: %x", data)
		}
	}

	lossy := make([]byte, 10)
	copy(lossy[3:6], []byte{0x9d, 0x01, 0x2a})
	binary.LittleEndian.PutUint16(lossy[6:8], 2)
	binary.LittleEndian.PutUint16(lossy[8:10], 3)
	if width, height, animated, err := inspectPromptWebP(webp("VP8 ", lossy)); err != nil ||
		width != 2 || height != 3 || animated {
		t.Fatalf("lossy WebP = (%d, %d, %t, %v)", width, height, animated, err)
	}

	binary.LittleEndian.PutUint16(lossy[6:8], 0)
	if _, _, _, err := inspectPromptWebP(webp("VP8 ", lossy)); err == nil {
		t.Fatal("zero-width lossy WebP accepted")
	}

	lossless := []byte{0x2f, 1, 0, 0, 0}
	if width, height, animated, err := inspectPromptWebP(webp("VP8L", lossless)); err != nil ||
		width != 2 || height != 1 || animated {
		t.Fatalf("lossless WebP = (%d, %d, %t, %v)", width, height, animated, err)
	}
}
