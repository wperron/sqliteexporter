// Copyright 2024 William Perron. All rights reserved. MIT License.
package sqliteexporter

import (
	"errors"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
)

var _ component.Config = (*Config)(nil)

type Config struct {
	// Path of the sqlite3 database file. Path is relative to current directory.
	// If file does not exist, it will be created by the exporter.
	Path string `mapstructure:"path"`

	// TODO(wperron) add options for WAL/journal mode, etc.

	// TODO(wperron) add option of "hoisted fields" like service name and duration
}

func (cfg *Config) Validate() error {
	if cfg.Path == "" {
		return errors.New("path must be non-empty")
	}

	return nil
}

func (cfg *Config) Unmarshal(componentParser *confmap.Conf) error {
	if componentParser == nil {
		return errors.New("empty config for sqlite exporter")
	}

	if err := componentParser.Unmarshal(cfg, confmap.WithErrorUnused()); err != nil {
		return err
	}

	return nil
}
