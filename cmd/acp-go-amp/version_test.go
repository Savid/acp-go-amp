package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVersionReturnsBuildVersion(t *testing.T) {
	original := buildVersion
	t.Cleanup(func() { buildVersion = original })

	buildVersion = "v1.2.3"
	require.Equal(t, "v1.2.3", version())
}

func TestVersionDefaultsToDevWhenEmpty(t *testing.T) {
	original := buildVersion
	t.Cleanup(func() { buildVersion = original })

	buildVersion = ""
	require.Equal(t, "dev", version())
}
