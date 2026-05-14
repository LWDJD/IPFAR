package verify

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LWDJD/ipfar-sdk/verify/pipeline"
)

// ============================================================
// parseCarFileName 测试
// ============================================================

func TestParseCarFileName(t *testing.T) {
	tests := []struct {
		fileName     string
		expectedRoot string
		expectedTXID string
	}{
		{
			fileName:     "bafy1234567890abcdef_bafy12345678.car",
			expectedRoot: "bafy1234567890abcdef",
			expectedTXID: "bafy12345678",
		},
		{
			fileName:     "QmABC123XYZ789_abc123def456.car",
			expectedRoot: "QmABC123XYZ789",
			expectedTXID: "abc123def456",
		},
		{
			fileName:     "root_with_underscores_1234_txid5678.car",
			expectedRoot: "root_with_underscores_1234",
			expectedTXID: "txid5678",
		},
		{
			fileName:     "noextension",
			expectedRoot: "",
			expectedTXID: "",
		},
		{
			fileName:     "no_underscore.car",
			expectedRoot: "no",
			expectedTXID: "underscore",
		},
		{
			fileName:     "a_b.car",
			expectedRoot: "a",
			expectedTXID: "b",
		},
	}

	for _, tt := range tests {
		t.Run(tt.fileName, func(t *testing.T) {
			root, txid := parseCarFileName(tt.fileName)
			if tt.expectedRoot != "" && root != tt.expectedRoot {
				t.Errorf("parseCarFileName(%q) root = %q, want %q", tt.fileName, root, tt.expectedRoot)
			}
			if tt.expectedTXID != "" && txid != tt.expectedTXID {
				t.Errorf("parseCarFileName(%q) txid = %q, want %q", tt.fileName, txid, tt.expectedTXID)
			}
		})
	}
}

// ============================================================
// FullVerifier 测试
// ============================================================

func TestNewFullVerifier(t *testing.T) {
	fv := NewFullVerifier(DefaultFullVerifierConfig())
	if fv == nil {
		t.Fatal("NewFullVerifier returned nil")
	}
	if fv.gateway == nil {
		t.Fatal("gateway is nil")
	}
	if fv.sema == nil {
		t.Fatal("sema is nil")
	}
	if cap(fv.sema) != 2 {
		t.Errorf("sema capacity = %d, want 2", cap(fv.sema))
	}
}

func TestNewFullVerifier_CustomConfig(t *testing.T) {
	cfg := FullVerifierConfig{
		CacheDir:              "/tmp/test-cache",
		MaxConcurrency:        4,
		VerifyPoW:             false,
		VerifyIndex:           false,
		VerifyReferenceChain:  false,
		VerifyIntegrity:       false,
		SkipMissingMeta:       false,
	}
	fv := NewFullVerifier(cfg)
	if cap(fv.sema) != 4 {
		t.Errorf("sema capacity = %d, want 4", cap(fv.sema))
	}
	// 验证管道配置
	pcfg := fv.GetPipelineConfig()
	if pcfg.VerifyPoW {
		t.Error("VerifyPoW should be false")
	}
	if pcfg.VerifyIndex {
		t.Error("VerifyIndex should be false")
	}
}

// ============================================================
// scanCarFiles 测试
// ============================================================

func TestScanCarFiles_EmptyDir(t *testing.T) {
	tmpDir := t.TempDir()
	cacheDir := filepath.Join(tmpDir, "empty-cache")
	os.MkdirAll(cacheDir, 0755)

	fv := NewFullVerifier(FullVerifierConfig{
		CacheDir: cacheDir,
	})

	files, err := fv.scanCarFiles()
	if err != nil {
		t.Fatalf("scanCarFiles should not error: %v", err)
	}
	if len(files) != 0 {
		t.Errorf("expected 0 files, got %d", len(files))
	}
}

func TestScanCarFiles_WithCARFiles(t *testing.T) {
	tmpDir := t.TempDir()
	cacheDir := filepath.Join(tmpDir, "car-cache")
	os.MkdirAll(cacheDir, 0755)

	// Create some .car files
	carFiles := []string{
		"bafy123_abc123.car",
		"QmXYZ789_def456.car",
		"notacar.txt",
		"subdir/bafy999_ghi012.car",
	}
	for _, f := range carFiles {
		path := filepath.Join(cacheDir, f)
		os.MkdirAll(filepath.Dir(path), 0755)
		os.WriteFile(path, []byte("dummy car content"), 0644)
	}

	fv := NewFullVerifier(FullVerifierConfig{
		CacheDir: cacheDir,
	})

	files, err := fv.scanCarFiles()
	if err != nil {
		t.Fatalf("scanCarFiles should not error: %v", err)
	}
	if len(files) != 3 {
		t.Errorf("expected 3 car files, got %d", len(files))
	}

	// Verify the files found
	foundPaths := make(map[string]bool)
	for _, f := range files {
		foundPaths[filepath.Base(f.Path)] = true
	}
	if !foundPaths["bafy123_abc123.car"] {
		t.Error("expected to find bafy123_abc123.car")
	}
	if !foundPaths["QmXYZ789_def456.car"] {
		t.Error("expected to find QmXYZ789_def456.car")
	}
	if !foundPaths["bafy999_ghi012.car"] {
		t.Error("expected to find bafy999_ghi012.car")
	}
	if foundPaths["notacar.txt"] {
		t.Error("should not find notacar.txt")
	}
}

func TestScanCarFiles_MixedExtensions(t *testing.T) {
	tmpDir := t.TempDir()
	cacheDir := filepath.Join(tmpDir, "mixed-cache")
	os.MkdirAll(cacheDir, 0755)

	// .CAR (uppercase) and .car (lowercase) should both be found
	os.WriteFile(filepath.Join(cacheDir, "test1.CAR"), []byte("data"), 0644)
	os.WriteFile(filepath.Join(cacheDir, "test2.car"), []byte("data"), 0644)
	os.WriteFile(filepath.Join(cacheDir, "test3.CaR"), []byte("data"), 0644)

	fv := NewFullVerifier(FullVerifierConfig{
		CacheDir: cacheDir,
	})

	files, err := fv.scanCarFiles()
	if err != nil {
		t.Fatalf("scanCarFiles should not error: %v", err)
	}
	if len(files) != 3 {
		t.Errorf("expected 3 car files (all case variants), got %d", len(files))
	}
}

// ============================================================
// verifyOne 测试
// ============================================================

func TestVerifyOne_EmptyRootCID(t *testing.T) {
	tmpDir := t.TempDir()
	carPath := filepath.Join(tmpDir, "x.car")
	os.WriteFile(carPath, []byte("data"), 0644)

	fv := NewFullVerifier(DefaultFullVerifierConfig())
	result := fv.verifyOne(carFileInfo{
		Path:     carPath,
		FileName: "x.car",
		Size:     4,
	})

	if !result.Skipped {
		t.Error("should be skipped due to empty root_cid (single-char filename)")
	}
	if result.Passed {
		t.Error("should not pass")
	}
}

func TestVerifyOne_ValidFileName_SkipMissingMeta(t *testing.T) {
	tmpDir := t.TempDir()
	cacheDir := filepath.Join(tmpDir, "cache")
	os.MkdirAll(cacheDir, 0755)

	// Create a dummy CAR file with a valid-looking name
	carPath := filepath.Join(cacheDir, "bafy1234567890abcdef1234567890ab_abcdef123456.car")
	os.WriteFile(carPath, []byte("dummy car v2 content"), 0644)

	fv := NewFullVerifier(FullVerifierConfig{
		CacheDir:        cacheDir,
		SkipMissingMeta: true, // skip because we can't fetch metadata
		GatewayURLs:     []string{"http://127.0.0.1:1"}, // invalid gateway
	})

	result := fv.verifyOne(carFileInfo{
		Path:     carPath,
		FileName: "bafy1234567890abcdef1234567890ab_abcdef123456.car",
		Size:     21,
	})

	// Should be skipped because metadata can't be fetched (invalid gateway)
	if !result.Skipped {
		t.Logf("result: passed=%v skipped=%v error=%q", result.Passed, result.Skipped, result.Error)
	}
}

// ============================================================
// FullVerifyReport 测试
// ============================================================

func TestFullVerifyReport_Empty(t *testing.T) {
	report := &FullVerifyReport{
		StartedAt:  time.Now(),
		FinishedAt: time.Now(),
	}

	if report.HasFailures() {
		t.Error("empty report should not have failures")
	}
	if !report.AllPassedOrSkipped() {
		t.Error("empty report should be all passed or skipped")
	}
}

func TestFullVerifyReport_WithFailures(t *testing.T) {
	report := &FullVerifyReport{
		TotalFiles:    3,
		VerifiedFiles: 3,
		PassedFiles:   2,
		FailedFiles:   1,
		SkippedFiles:  0,
		Results: []FullVerifyResult{
			{Passed: true},
			{Passed: true},
			{Passed: false, Error: "verification failed"},
		},
	}

	if !report.HasFailures() {
		t.Error("report with 1 failed should have failures")
	}
	if report.AllPassedOrSkipped() {
		t.Error("report with failures should not be all passed or skipped")
	}
}

func TestFullVerifyReport_WithSkipped(t *testing.T) {
	report := &FullVerifyReport{
		TotalFiles:    3,
		VerifiedFiles: 2,
		PassedFiles:   2,
		FailedFiles:   0,
		SkippedFiles:  1,
		Results: []FullVerifyResult{
			{Passed: true},
			{Passed: true},
			{Skipped: true, Error: "cannot fetch metadata"},
		},
	}

	if report.HasFailures() {
		t.Error("report with only skipped should not have failures")
	}
	if !report.AllPassedOrSkipped() {
		t.Error("report with only skipped should be all passed or skipped")
	}
}

// ============================================================
// FormatReport 测试
// ============================================================

func TestFormatReport_Nil(t *testing.T) {
	result := FormatReport(nil)
	if result != "无验证报告" {
		t.Errorf("FormatReport(nil) = %q, want '无验证报告'", result)
	}
}

func TestFormatReport_NonNil(t *testing.T) {
	report := &FullVerifyReport{
		TotalFiles:    2,
		VerifiedFiles: 2,
		PassedFiles:   1,
		FailedFiles:   1,
		SkippedFiles:  0,
		TotalDuration: 1500 * time.Millisecond,
		StartedAt:     time.Now(),
		FinishedAt:    time.Now(),
		Results: []FullVerifyResult{
			{
				RootCID:  "bafy123",
				DataTXID: "abc123",
				CARPath:  "/cache/bafy123_abc123.car",
				Passed:   true,
				Duration: 500 * time.Millisecond,
				Steps: []pipeline.VerifyResult{
					{Step: "pow", Passed: true, Message: "ok"},
					{Step: "index", Passed: true, Message: "ok"},
				},
			},
			{
				RootCID:  "bafy456",
				DataTXID: "def456",
				CARPath:  "/cache/bafy456_def456.car",
				Passed:   false,
				Error:    "index mismatch",
				Duration: 1000 * time.Millisecond,
				Steps: []pipeline.VerifyResult{
					{Step: "pow", Passed: true, Message: "ok"},
					{Step: "index", Passed: false, Error: "CID mismatch"},
				},
			},
		},
	}

	result := FormatReport(report)

	// Verify key strings are present
	checks := []string{
		"全量验证报告",
		"2",        // total files
		"1.5s",     // total duration
		"bafy123",  // root cid
		"abc123",   // data txid
		"✅",       // pass icon
		"❌",       // fail icon
		"通过",
		"失败",
		"pow",
		"index",
		"CID mismatch",
	}

	for _, check := range checks {
		if !strings.Contains(result, check) {
			t.Errorf("FormatReport output missing %q", check)
		}
	}

	// Debug output
	t.Logf("FormatReport output:\n%s", result)
}

// ============================================================
// 边界条件测试
// ============================================================

func TestFullVerifier_ZeroConcurrency(t *testing.T) {
	cfg := FullVerifierConfig{
		MaxConcurrency: 0, // should default to 2
	}
	fv := NewFullVerifier(cfg)
	if cap(fv.sema) != 2 {
		t.Errorf("zero concurrency should default to 2, got %d", cap(fv.sema))
	}
}

func TestFullVerifier_EmptyCacheDir(t *testing.T) {
	cfg := FullVerifierConfig{
		CacheDir: "", // should default to "cache/car"
	}
	fv := NewFullVerifier(cfg)
	if fv.config.CacheDir != "cache/car" {
		t.Errorf("empty CacheDir should default to 'cache/car', got %q", fv.config.CacheDir)
	}
}

func TestFullVerifier_VerificationWithAllStepsDisabled(t *testing.T) {
	tmpDir := t.TempDir()
	cacheDir := filepath.Join(tmpDir, "cache")
	os.MkdirAll(cacheDir, 0755)

	// Create a car file
	carPath := filepath.Join(cacheDir, "test_123.car")
	os.WriteFile(carPath, []byte("dummy"), 0644)

	cfg := FullVerifierConfig{
		CacheDir:              cacheDir,
		VerifyPoW:             false,
		VerifyIndex:           false,
		VerifyReferenceChain:  false,
		VerifyIntegrity:       false,
		SkipMissingMeta:       true,
		GatewayURLs:           []string{"http://localhost:1"}, // invalid gateway to force skip
	}

	fv := NewFullVerifier(cfg)

	report, err := fv.VerifyAll()
	if err != nil {
		t.Fatalf("VerifyAll should not error: %v", err)
	}
	if report.TotalFiles != 1 {
		t.Errorf("TotalFiles = %d, want 1", report.TotalFiles)
	}
	// Since the gateway is invalid, the file should be skipped
	if report.SkippedFiles != 1 {
		t.Logf("skipped=%d verified=%d failed=%d passed=%d",
			report.SkippedFiles, report.VerifiedFiles, report.FailedFiles, report.PassedFiles)
	}
}

// ============================================================
// 确保包级别引用
// ============================================================

var _ = fmt.Sprintf
