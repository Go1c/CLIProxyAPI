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
  originator: "  my-originator  "
  version: "  1.2.3  "
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
	if got := cfg.CodexHeaderDefaults.Originator; got != "my-originator" {
		t.Fatalf("Originator = %q, want %q", got, "my-originator")
	}
	if got := cfg.CodexHeaderDefaults.Version; got != "1.2.3" {
		t.Fatalf("Version = %q, want %q", got, "1.2.3")
	}
	if got := cfg.CodexHeaderDefaults.BetaFeatures; got != "feature-a,feature-b" {
		t.Fatalf("BetaFeatures = %q, want %q", got, "feature-a,feature-b")
	}
	if cfg.Codex.DisableCodexCloaking {
		t.Fatal("DisableCodexCloaking = true, want default false")
	}
}

func TestLoadConfigOptional_CodexHeaderDefaultsUsesRepositoryDefaults(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(configPath, []byte("port: 8080\n"), 0o600); err != nil {
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
	if got := cfg.CodexHeaderDefaults.Version; got != DefaultCodexHeaderVersion {
		t.Fatalf("Version = %q, want %q", got, DefaultCodexHeaderVersion)
	}
	if got := cfg.CodexHeaderDefaults.BetaFeatures; got != DefaultCodexHeaderBetaFeatures {
		t.Fatalf("BetaFeatures = %q, want %q", got, DefaultCodexHeaderBetaFeatures)
	}
}

func TestLoadConfigOptional_CodexIdentityConfuse(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	configYAML := []byte(`
codex:
  identity-confuse: true
  disable-codex-cloaking: true
  optimize-multi-agent-v2: true
`)
	if err := os.WriteFile(configPath, configYAML, 0o600); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	cfg, err := LoadConfigOptional(configPath, false)
	if err != nil {
		t.Fatalf("LoadConfigOptional() error = %v", err)
	}

	if !cfg.Codex.IdentityConfuse {
		t.Fatalf("IdentityConfuse = false, want true")
	}
	if !cfg.Codex.DisableCodexCloaking {
		t.Fatal("DisableCodexCloaking = false, want true")
	}
	if !cfg.Codex.OptimizeMultiAgentV2 {
		t.Fatalf("OptimizeMultiAgentV2 = false, want true")
	}
}
