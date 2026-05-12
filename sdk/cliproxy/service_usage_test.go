package cliproxy

import (
	"path/filepath"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

func TestUsageStatisticsPathRequiresLegacyAggregation(t *testing.T) {
	s := &Service{
		cfg: &config.Config{
			UsageStatisticsEnabled:            true,
			UsageStatisticsAggregationEnabled: false,
			UsageStatisticsPath:               filepath.Join(t.TempDir(), "usage-statistics.json"),
		},
	}

	if got := s.usageStatisticsPath(); got != "" {
		t.Fatalf("usageStatisticsPath() = %q, want empty when legacy aggregation is disabled", got)
	}
}

func TestUsageStatisticsPathUsesExplicitPathWhenLegacyAggregationEnabled(t *testing.T) {
	want := filepath.Join(t.TempDir(), "usage-statistics.json")
	s := &Service{
		cfg: &config.Config{
			UsageStatisticsEnabled:            true,
			UsageStatisticsAggregationEnabled: true,
			UsageStatisticsPath:               want,
		},
	}

	if got := s.usageStatisticsPath(); got != want {
		t.Fatalf("usageStatisticsPath() = %q, want %q", got, want)
	}
}
