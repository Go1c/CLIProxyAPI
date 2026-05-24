package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigOptional_CodexHeaderDefaults(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	configYAML := []byte(`
codex-header-defaults:
  user-agent: "  my-codex-client/1.0  "
  originator: "  my-codex-origin  "
  beta-features: "  feature-a,feature-b  "
`)
	if err := os.WriteFile(configPath, configYAML, 0o600); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	cfg, err := LoadConfigOptional(configPath, false)
	if err != nil {
		t.Fatalf("LoadConfigOptional() error = %v", err)
	}

	if got := cfg.CodexHeaderDefaults.UserAgent; got != "my-codex-client/1.0" {
		t.Fatalf("UserAgent = %q, want %q", got, "my-codex-client/1.0")
	}
	if got := cfg.CodexHeaderDefaults.Originator; got != "my-codex-origin" {
		t.Fatalf("Originator = %q, want %q", got, "my-codex-origin")
	}
	if got := cfg.CodexHeaderDefaults.BetaFeatures; got != "feature-a,feature-b" {
		t.Fatalf("BetaFeatures = %q, want %q", got, "feature-a,feature-b")
	}
}

func TestLoadConfigOptional_CodexHeaderDefaultsUsesVSCodeProfile(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(`{}`), 0o600); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	cfg, err := LoadConfigOptional(configPath, false)
	if err != nil {
		t.Fatalf("LoadConfigOptional() error = %v", err)
	}

	if got := cfg.CodexHeaderDefaults.UserAgent; got != DefaultCodexHeaderUserAgent {
		t.Fatalf("UserAgent = %q, want %q", got, DefaultCodexHeaderUserAgent)
	}
	if got := cfg.CodexHeaderDefaults.Originator; got != DefaultCodexHeaderOriginator {
		t.Fatalf("Originator = %q, want %q", got, DefaultCodexHeaderOriginator)
	}
	if got := cfg.CodexHeaderDefaults.BetaFeatures; got != DefaultCodexHeaderBetaFeatures {
		t.Fatalf("BetaFeatures = %q, want %q", got, DefaultCodexHeaderBetaFeatures)
	}
}
