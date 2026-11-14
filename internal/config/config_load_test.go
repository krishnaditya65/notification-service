package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The shipped configs/config.yaml must load: durations parse and pool sizes reach the struct.
func TestShippedConfigLoads(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}

	previous, _ := os.Getwd()
	defer os.Chdir(previous)

	if err := os.Chdir(root); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load()
	if err != nil {
		t.Fatalf("shipped config does not load: %v", err)
	}

	if cfg.Database.MaxOpenConns < 1 || cfg.Server.ReadTimeout != 30*time.Second || cfg.Reliability.DLQ.Retention <= 0 {
		t.Fatalf("config values not decoded: %+v %+v", cfg.Database, cfg.Server)
	}
}
