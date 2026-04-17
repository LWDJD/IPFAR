package version

import (
	"strings"
	"testing"
)

func TestVersionDefaults(t *testing.T) {
	if Version != "dev" {
		t.Errorf("默认 Version 应为 dev，得到 %s", Version)
	}
	if GitCommit != "unknown" {
		t.Errorf("默认 GitCommit 应为 unknown，得到 %s", GitCommit)
	}
	if BuildDate != "unknown" {
		t.Errorf("默认 BuildDate 应为 unknown，得到 %s", BuildDate)
	}
}

func TestVersionString(t *testing.T) {
	s := String()
	if !strings.Contains(s, "IPFAR") {
		t.Error("Version.String() 应包含 IPFAR")
	}
	if !strings.Contains(s, Version) {
		t.Error("Version.String() 应包含版本号")
	}
	if !strings.Contains(s, GitCommit) {
		t.Error("Version.String() 应包含 commit")
	}
	if !strings.Contains(s, BuildDate) {
		t.Error("Version.String() 应包含构建日期")
	}
}

func TestVersionShort(t *testing.T) {
	s := Short()
	if s != Version {
		t.Errorf("Short() 应返回 %s，得到 %s", Version, s)
	}
}

func TestVersionOverride(t *testing.T) {
	origVersion := Version
	origCommit := GitCommit
	origDate := BuildDate

	Version = "v1.2.3"
	GitCommit = "abc123"
	BuildDate = "2026-01-01"

	s := String()
	if !strings.Contains(s, "v1.2.3") {
		t.Error("覆盖后应包含 v1.2.3")
	}
	if !strings.Contains(s, "abc123") {
		t.Error("覆盖后应包含 abc123")
	}
	if !strings.Contains(s, "2026-01-01") {
		t.Error("覆盖后应包含 2026-01-01")
	}

	Version = origVersion
	GitCommit = origCommit
	BuildDate = origDate
}
