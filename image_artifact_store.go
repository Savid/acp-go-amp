package ampacp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	imageArtifactPrefix  = "_artifacts/images/"
	imageArtifactVersion = 1
	imageArtifactTTL     = 24 * time.Hour

	imageArtifactKindEmbedded = "embedded"
	imageArtifactKindLink     = "resource_link"
)

var imageArtifactNow = time.Now

type storedImageArtifact struct {
	Version     int    `json:"version"`
	Kind        string `json:"kind"`
	Identity    string `json:"identity"`
	Fingerprint string `json:"fingerprint"`
	MimeType    string `json:"mimeType,omitempty"`
	Data        string `json:"data,omitempty"`
	URI         string `json:"uri,omitempty"`
	CreatedAt   int64  `json:"createdAt"`
}

func imageArtifactSubpath(identity, fingerprint string) string {
	sum := sha256.Sum256([]byte(identity + "\x00" + fingerprint))

	return imageArtifactPrefix + hex.EncodeToString(sum[:]) + ".json"
}

func isImageArtifactSubpath(subpath string) bool {
	return strings.HasPrefix(subpath, imageArtifactPrefix)
}

func (s *agentSession) persistImageArtifact(
	ctx context.Context,
	artifact storedImageArtifact,
) (string, error) {
	artifact.Version = imageArtifactVersion
	subpath := imageArtifactSubpath(artifact.Identity, artifact.Fingerprint)
	key := SessionKey{SessionID: string(s.id), Subpath: subpath}

	loadCtx, cancelLoad := s.agent.sessionStoreLoadContext(ctx)
	entries, err := s.agent.store.Load(loadCtx, key)

	cancelLoad()

	if err != nil {
		return "", fmt.Errorf("load image artifact: %w", err)
	}

	if len(entries) > 0 {
		existing, decodeErr := decodeStoredImageArtifact(entries[len(entries)-1])
		if decodeErr != nil || !sameStoredImageArtifact(existing, artifact) {
			return "", errors.New("stored image artifact conflicts with native identity")
		}

		return subpath, nil
	}

	artifact.CreatedAt = imageArtifactNow().UnixMilli()

	entry, _ := json.Marshal(artifact)

	writeCtx, cancelWrite := s.agent.sessionStoreWriteContext(ctx)
	err = s.agent.store.Append(writeCtx, key, []SessionStoreEntry{entry})

	cancelWrite()

	if err != nil {
		return "", fmt.Errorf("store image artifact: %w", err)
	}

	return subpath, nil
}

func sameStoredImageArtifact(left, right storedImageArtifact) bool {
	return left.Version == right.Version &&
		left.Kind == right.Kind &&
		left.Identity == right.Identity &&
		left.Fingerprint == right.Fingerprint &&
		left.MimeType == right.MimeType &&
		left.Data == right.Data &&
		left.URI == right.URI
}

func (s *agentSession) loadImageArtifact(ctx context.Context, subpath string) (storedImageArtifact, error) {
	if !isImageArtifactSubpath(subpath) {
		return storedImageArtifact{}, errors.New("invalid image artifact reference")
	}

	key := SessionKey{SessionID: string(s.id), Subpath: subpath}
	loadCtx, cancelLoad := s.agent.sessionStoreLoadContext(ctx)
	entries, err := s.agent.store.Load(loadCtx, key)

	cancelLoad()

	if err != nil {
		return storedImageArtifact{}, fmt.Errorf("load image artifact: %w", err)
	}

	if len(entries) == 0 {
		return storedImageArtifact{}, errors.New("image artifact is no longer available")
	}

	artifact, err := decodeStoredImageArtifact(entries[len(entries)-1])
	if err != nil {
		return storedImageArtifact{}, err
	}

	if artifact.CreatedAt < imageArtifactNow().Add(-imageArtifactTTL).UnixMilli() {
		writeCtx, cancelWrite := s.agent.sessionStoreWriteContext(ctx)
		deleteErr := s.agent.store.Delete(writeCtx, key)

		cancelWrite()

		if deleteErr != nil {
			return storedImageArtifact{}, fmt.Errorf("delete expired image artifact: %w", deleteErr)
		}

		return storedImageArtifact{}, errors.New("image artifact expired")
	}

	expected := imageArtifactSubpath(artifact.Identity, artifact.Fingerprint)
	if expected != subpath {
		return storedImageArtifact{}, errors.New("image artifact reference does not match its identity")
	}

	return artifact, nil
}

func decodeStoredImageArtifact(entry SessionStoreEntry) (storedImageArtifact, error) {
	var artifact storedImageArtifact

	if err := json.Unmarshal(entry, &artifact); err != nil {
		return storedImageArtifact{}, errors.New("image artifact record is invalid")
	}

	if artifact.Version != imageArtifactVersion ||
		artifact.Identity == "" ||
		artifact.Fingerprint == "" ||
		artifact.CreatedAt <= 0 {
		return storedImageArtifact{}, errors.New("image artifact record is invalid")
	}

	switch artifact.Kind {
	case imageArtifactKindEmbedded:
		if artifact.Data == "" || artifact.MimeType == "" || artifact.URI != "" {
			return storedImageArtifact{}, errors.New("embedded image artifact record is invalid")
		}
	case imageArtifactKindLink:
		if artifact.URI == "" ||
			artifact.Data != "" ||
			!isRemoteImageURI(artifact.URI) ||
			fingerprintImageOutput([]byte(artifact.URI)) != artifact.Fingerprint {
			return storedImageArtifact{}, errors.New("resource-link image artifact record is invalid")
		}
	default:
		return storedImageArtifact{}, errors.New("image artifact kind is invalid")
	}

	return artifact, nil
}

func (s *agentSession) imageArtifactReplacements(ctx context.Context) ([]SessionStoreReplacement, error) {
	loadCtx, cancelList := s.agent.sessionStoreLoadContext(ctx)
	subkeys, err := s.agent.store.ListSubkeys(
		loadCtx,
		SessionKey{SessionID: string(s.id), Subpath: SessionStoreMainSubpath},
	)

	cancelList()

	if err != nil {
		return nil, fmt.Errorf("list image artifacts: %w", err)
	}

	replacements := make([]SessionStoreReplacement, 0)

	for _, subpath := range subkeys {
		if !isImageArtifactSubpath(subpath) {
			continue
		}

		key := SessionKey{SessionID: string(s.id), Subpath: subpath}
		loadCtx, cancelLoad := s.agent.sessionStoreLoadContext(ctx)
		entries, err := s.agent.store.Load(loadCtx, key)

		cancelLoad()

		if err != nil {
			return nil, fmt.Errorf("load image artifact for commit: %w", err)
		}

		if len(entries) == 0 {
			continue
		}

		replacements = append(replacements, SessionStoreReplacement{Key: key, Entries: entries})
	}

	return replacements, nil
}

func (a *Agent) sweepExpiredImageArtifacts(ctx context.Context) error {
	loadCtx, cancelSessions := a.sessionStoreLoadContext(ctx)
	sessions, err := a.store.ListSessions(loadCtx)

	cancelSessions()

	if err != nil {
		return fmt.Errorf("list sessions for image artifact sweep: %w", err)
	}

	cutoff := imageArtifactNow().Add(-imageArtifactTTL).UnixMilli()

	for _, session := range sessions {
		main := SessionKey{SessionID: session.SessionID, Subpath: SessionStoreMainSubpath}
		loadCtx, cancelList := a.sessionStoreLoadContext(ctx)
		subkeys, err := a.store.ListSubkeys(loadCtx, main)

		cancelList()

		if err != nil {
			return fmt.Errorf("list image artifacts for sweep: %w", err)
		}

		for _, subpath := range subkeys {
			if !isImageArtifactSubpath(subpath) {
				continue
			}

			key := SessionKey{SessionID: session.SessionID, Subpath: subpath}
			loadCtx, cancelLoad := a.sessionStoreLoadContext(ctx)
			entries, err := a.store.Load(loadCtx, key)

			cancelLoad()

			if err != nil {
				return fmt.Errorf("load image artifact for sweep: %w", err)
			}

			if len(entries) == 0 {
				continue
			}

			artifact, decodeErr := decodeStoredImageArtifact(entries[len(entries)-1])
			if decodeErr == nil && artifact.CreatedAt >= cutoff {
				continue
			}

			writeCtx, cancelDelete := a.sessionStoreWriteContext(ctx)
			err = a.store.Delete(writeCtx, key)

			cancelDelete()

			if err != nil {
				return fmt.Errorf("delete expired image artifact: %w", err)
			}
		}
	}

	return nil
}
