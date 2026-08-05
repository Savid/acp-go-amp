package ampacp

func handoffGeneratedNativeTree(root string, isolation *ProcessIsolation) error {
	if isolation == nil {
		return nil
	}

	return handoffGeneratedNativeTreePlatform(root, isolation.UID, isolation.GID)
}

func writeNativeOwnedFile(path string, contents []byte, isolation *ProcessIsolation) error {
	if isolation == nil {
		return nil
	}

	return writeNativeOwnedFilePlatform(path, contents, isolation.UID, isolation.GID)
}
