// Copyright 2024 William Perron. All rights reserved. MIT License.
package sqliteexporter

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"
)

func TestCreateDefaultConfig(t *testing.T) {
	cfg := createDefaultConfig()
	assert.NotNil(t, cfg, "failed to create default config")
	assert.NoError(t, componenttest.CheckConfigStruct(cfg))
}

func Test_createTracesExporter(t *testing.T) {
	cfg := &Config{
		Path: "./traces.db",
	}

	exp, err := createTracesExporter(
		context.Background(),
		exportertest.NewNopCreateSettings(),
		cfg,
	)
	assert.NoError(t, err)
	require.NotNil(t, exp)

	os.Remove("./traces.db")
}
