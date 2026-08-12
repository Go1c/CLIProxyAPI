package config

const (
	DefaultPanelGitHubRepository = "https://github.com/router-for-me/Cli-Proxy-API-Management-Center"
	DefaultPprofAddr             = "127.0.0.1:8316"
	DefaultAuthDir               = "~/.cli-proxy-api"
)

const (
	DefaultCodexHeaderUserAgent    = "codex-tui/0.146.0 (Mac OS 26.5.2; arm64) Orca/1.4.178 (codex-tui; 0.146.0)"
	DefaultCodexHeaderOriginator   = "codex-tui"
	DefaultCodexHeaderVersion      = "0.146.0"
	DefaultCodexHeaderBetaFeatures = "remote_compaction_v2"
)

// DefaultCodexHeaderDefaults returns the Codex identity header defaults aligned
// to the local Codex CLI 0.146.0 profile.
func DefaultCodexHeaderDefaults() CodexHeaderDefaults {
	return CodexHeaderDefaults{
		UserAgent:    DefaultCodexHeaderUserAgent,
		Originator:   DefaultCodexHeaderOriginator,
		Version:      DefaultCodexHeaderVersion,
		BetaFeatures: DefaultCodexHeaderBetaFeatures,
	}
}
