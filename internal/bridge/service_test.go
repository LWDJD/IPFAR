package bridge

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sdkmeta "github.com/LWDJD/ipfar-sdk/verify/metadata"
	"github.com/LWDJD/ipfar-sdk/verify/pipeline"

	"github.com/lwdjd/IPFAR/config"
	"github.com/lwdjd/IPFAR/internal/discovery"
	"github.com/lwdjd/IPFAR/internal/download"
	"github.com/lwdjd/IPFAR/internal/log"
)

func init() {
	// 初始化最小日志系统
	log.Init(log.Config{
		Level:      log.ERROR,
		FilePath:   "",
		UseConsole: false,
		UseJSON:    false,
	})
}

// setupMockServer 创建模拟 Arweave 网关
func setupMockServer(t *testing.T, metaData sdkmeta.Metadata, carData []byte) *httptest.Server {
	t.Helper()

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case strings.Contains(path, "/meta-tx"):
			jsonBytes, _ := json.Marshal(metaData)
			w.Header().Set("Content-Type", "application/json")
			w.Write(jsonBytes)
		case strings.Contains(path, "/"+metaData.DataTXID):
			w.Header().Set("Content-Type", "application/vnd.ipld.car")
			w.Write(carData)
		case strings.Contains(path, "/tx-404"):
			w.WriteHeader(http.StatusNotFound)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

func TestNewService(t *testing.T) {
	cfg := DefaultServiceConfig()
	cfg.Preset = pipeline.SecurityLight

	svc, err := NewService(cfg)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}
	if svc == nil {
		t.Fatal("NewService returned nil")
	}
	if svc.bridge == nil {
		t.Error("bridge is nil")
	}
	if svc.fetcher == nil {
		t.Error("fetcher is nil")
	}
}

func TestNewService_InvalidPreset(t *testing.T) {
	cfg := DefaultServiceConfig()
	cfg.Preset = "invalid"

	_, err := NewService(cfg)
	if err == nil {
		t.Error("NewService should fail with invalid preset")
	}
}

func TestNewService_AllPresets(t *testing.T) {
	presets := []string{
		pipeline.SecurityStrict,
		pipeline.SecurityBalanced,
		pipeline.SecurityLight,
		pipeline.SecurityTrusted,
	}

	for _, preset := range presets {
		t.Run(preset, func(t *testing.T) {
			cfg := DefaultServiceConfig()
			cfg.Preset = preset

			svc, err := NewService(cfg)
			if err != nil {
				t.Fatalf("NewService with preset %s failed: %v", preset, err)
			}

			bridgeCfg := svc.bridge.GetConfig()
			expected, _ := pipeline.GetPreset(preset)

			if bridgeCfg.VerifyPoW != expected.VerifyPoW {
				t.Errorf("VerifyPoW: got %v, want %v", bridgeCfg.VerifyPoW, expected.VerifyPoW)
			}
			if bridgeCfg.VerifyIndex != expected.VerifyIndex {
				t.Errorf("VerifyIndex: got %v, want %v", bridgeCfg.VerifyIndex, expected.VerifyIndex)
			}
			if bridgeCfg.VerifyReferenceChain != expected.VerifyReferenceChain {
				t.Errorf("VerifyReferenceChain: got %v, want %v", bridgeCfg.VerifyReferenceChain, expected.VerifyReferenceChain)
			}
			if bridgeCfg.VerifyIntegrity != expected.VerifyIntegrity {
				t.Errorf("VerifyIntegrity: got %v, want %v", bridgeCfg.VerifyIntegrity, expected.VerifyIntegrity)
			}
		})
	}
}

func TestService_ProcessMetadataFromJSON(t *testing.T) {
	cfg := DefaultServiceConfig()
	cfg.Preset = pipeline.SecurityTrusted // 最轻量
	cfg.CacheDir = t.TempDir()
	cfg.CarAvailable = false // 不下载 CAR

	svc, err := NewService(cfg)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	validJSON := fmt.Sprintf(`{
		"version": 1,
		"method": "raw",
		"root_cid": "bafkreiabcdefghijklmnopqrstuvwxyz1234567890",
		"data_txid": "txvaliddatatxid1234567890123456789012345678901234567890",
		"data_height": 1000,
		"data_size": 1024,
		"pow": "salt123",
		"pow_alg": "argon2idlightv1"
	}`)

	result, err := svc.ProcessMetadataFromJSON([]byte(validJSON))
	if err != nil {
		t.Fatalf("ProcessMetadataFromJSON failed: %v", err)
	}
	if result == nil {
		t.Fatal("result is nil")
	}
	if !result.Passed {
		t.Error("trusted preset should pass all checks")
	}
	if result.Meta == nil {
		t.Error("Meta is nil")
	}
}

func TestService_ProcessMetadataFromBase64(t *testing.T) {
	cfg := DefaultServiceConfig()
	cfg.Preset = pipeline.SecurityTrusted
	cfg.CacheDir = t.TempDir()
	cfg.CarAvailable = false

	svc, err := NewService(cfg)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	meta := &sdkmeta.Metadata{
		Version:    1,
		Method:     "raw",
		RootCID:    "bafkreiabcdefghijklmnopqrstuvwxyz1234567890",
		DataTXID:   "txbase64testtxid123456789012345678901234567890123456",
		DataHeight: 100,
		DataSize:   512,
		PoW:        "b64salt",
		PoWAlg:     "argon2idlightv1",
	}

	encoded, err := meta.ToBase64URL()
	if err != nil {
		t.Fatalf("ToBase64URL failed: %v", err)
	}

	result, err := svc.ProcessMetadataFromBase64(encoded)
	if err != nil {
		t.Fatalf("ProcessMetadataFromBase64 failed: %v", err)
	}
	if !result.Passed {
		t.Error("trusted preset should pass")
	}
}

func TestService_ProcessMetadataTX_WithMockServer(t *testing.T) {
	metaData := sdkmeta.Metadata{
		Version:    1,
		Method:     "raw",
		RootCID:    "bafkreitestservicetxidabcd1234abcd1234abcd1234ab",
		DataTXID:   "txservicetest1234567890123456789012345678901234",
		DataHeight: 500,
		DataSize:   2048,
		PoW:        "servicesalt",
		PoWAlg:     "argon2idlightv1",
	}

	carData := []byte("mock-car-binary-data-for-service-test")

	server := setupMockServer(t, metaData, carData)
	defer server.Close()

	cfg := DefaultServiceConfig()
	cfg.Preset = pipeline.SecurityTrusted
	cfg.CacheDir = t.TempDir()
	cfg.MaxFileSize = 10 * 1024 * 1024
	cfg.GatewayURLs = []string{server.URL}

	svc, err := NewService(cfg)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	result, err := svc.ProcessMetadataTX("meta-tx")
	if err != nil {
		t.Fatalf("ProcessMetadataTX failed: %v", err)
	}

	if result == nil {
		t.Fatal("result is nil")
	}
	if !result.Passed {
		t.Error("trusted preset should pass verification")
	}
	if result.Meta.RootCID != metaData.RootCID {
		t.Errorf("RootCID = %s, want %s", result.Meta.RootCID, metaData.RootCID)
	}

	// 验证 CAR 文件已下载（trusted 模式也下载 CAR，因为 CarAvailable=true）
	if cfg.CarAvailable && result.CARPath == "" {
		t.Error("CAR should have been downloaded")
	}
	if result.CARPath != "" {
		// Verify file exists
		if _, err := os.Stat(result.CARPath); os.IsNotExist(err) {
			t.Errorf("CAR file does not exist: %s", result.CARPath)
		}
	}
}

func TestService_ProcessMetadataTX_CARNotAvailable(t *testing.T) {
	metaData := sdkmeta.Metadata{
		Version:    1,
		Method:     "raw",
		RootCID:    "bafkreitestnocartxidabcd1234abcd1234abcd1234ab",
		DataTXID:   "txnocartest123456789012345678901234567890123456",
		DataHeight: 500,
		DataSize:   512,
		PoW:        "nocarsalt",
		PoWAlg:     "argon2idlightv1",
	}

	server := setupMockServer(t, metaData, []byte("car-data"))
	defer server.Close()

	cfg := DefaultServiceConfig()
	cfg.Preset = pipeline.SecurityTrusted
	cfg.CacheDir = t.TempDir()
	cfg.CarAvailable = false // 不下载 CAR
	cfg.GatewayURLs = []string{server.URL}

	svc, err := NewService(cfg)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	result, err := svc.ProcessMetadataTX("meta-tx")
	if err != nil {
		t.Fatalf("ProcessMetadataTX failed: %v", err)
	}

	if result.CARPath != "" {
		t.Error("CAR path should be empty when CarAvailable=false")
	}
	if !result.QuickOnly {
		t.Error("result should be quick-only")
	}
}

func TestService_Stats(t *testing.T) {
	cfg := DefaultServiceConfig()
	cfg.Preset = pipeline.SecurityTrusted
	cfg.CacheDir = t.TempDir()
	cfg.CarAvailable = false

	svc, err := NewService(cfg)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	stats := svc.Stats()
	if stats.MetadataFetched != 0 {
		t.Error("initial MetadataFetched should be 0")
	}
	if stats.PipelinesPassed != 0 {
		t.Error("initial PipelinesPassed should be 0")
	}

	// Process a metadata
	validJSON := `{
		"version": 1,
		"method": "bundle",
		"root_cid": "bafkreistatstestabcd1234abcd1234abcd1234abcd1234",
		"data_txid": "txstatstesttxid1234567890123456789012345678901234",
		"data_height": 100,
		"data_size": 99999999,
		"pow": "statssalt",
		"pow_alg": "argon2idlightv1"
	}`

	_, err = svc.ProcessMetadataFromJSON([]byte(validJSON))
	if err != nil {
		t.Fatalf("ProcessMetadataFromJSON failed: %v", err)
	}

	stats = svc.Stats()
	if stats.MetadataFetched != 1 {
		t.Errorf("MetadataFetched = %d, want 1", stats.MetadataFetched)
	}
	if stats.PipelinesPassed != 1 {
		t.Errorf("PipelinesPassed = %d, want 1", stats.PipelinesPassed)
	}
}

func TestFormatResult(t *testing.T) {
	meta := &sdkmeta.Metadata{
		Version:    1,
		Method:     "raw",
		RootCID:    "bafkreiformattestabcd1234abcd1234abcd1234abcd1234",
		DataTXID:   "txformattest123456789012345678901234567890123456789",
		DataHeight: 100,
		DataSize:   1024,
	}

	result := &PipelineResult{
		Meta:   meta,
		Passed: true,
		Steps: []pipeline.VerifyResult{
			{Step: "meta_validate", Passed: true},
			{Step: "pow", Passed: true, Skipped: true, Message: "file >= 100 MiB"},
		},
		CARPath: "/tmp/test.car",
	}

	output := FormatResult(result)
	if output == "" {
		t.Error("FormatResult returned empty string")
	}
	if !strings.Contains(output, "通过") {
		t.Error("output should contain 通过")
	}
	if !strings.Contains(output, "bafkreiformattest") {
		t.Error("output should contain root CID")
	}
}

func TestFormatResult_Nil(t *testing.T) {
	output := FormatResult(nil)
	if output != "无结果" {
		t.Errorf("FormatResult(nil) = %q, want 无结果", output)
	}
}

func TestService_SetSampler(t *testing.T) {
	cfg := DefaultServiceConfig()
	svc, err := NewService(cfg)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	checker := &mockBlockChecker{}
	sampler := discovery.NewSampler(discovery.SamplerConfig{
		MinHeight:    0,
		MaxHeight:    100,
		PollInterval: 0,
	}, checker)

	svc.SetSampler(sampler)
	if svc.sampler != sampler {
		t.Error("SetSampler did not set the sampler")
	}
}

func TestService_SetBlockChecker(t *testing.T) {
	cfg := DefaultServiceConfig()
	cfg.MinBlockHeight = 10
	cfg.MaxBlockHeight = 200
	cfg.PollInterval = 5 * time.Minute

	svc, err := NewService(cfg)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	checker := &mockBlockChecker{}
	svc.SetBlockChecker(checker)
	if svc.sampler == nil {
		t.Fatal("SetBlockChecker did not create sampler")
	}
}

// mockBlockChecker for tests
type mockBlockChecker struct{}

func (m *mockBlockChecker) CheckBlock(height uint64) (bool, []string, error) {
	return true, []string{fmt.Sprintf("tx-%d", height)}, nil
}

func TestConfigPresets(t *testing.T) {
	// 测试 config.GetVerifyConfig 的 4 档预设
	tests := []struct {
		preset    string
		wantPoW   bool
		wantIndex bool
		wantRef   bool
		wantInteg bool
	}{
		{"strict", true, true, true, true},
		{"balanced", true, true, false, true},
		{"light", true, false, true, false},
		{"trusted", false, false, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.preset, func(t *testing.T) {
			c := &config.Config{
				SecurityPreset: tt.preset,
			}
			pow, idx, ref, integ := c.GetVerifyConfig()
			if pow != tt.wantPoW {
				t.Errorf("pow = %v, want %v", pow, tt.wantPoW)
			}
			if idx != tt.wantIndex {
				t.Errorf("index = %v, want %v", idx, tt.wantIndex)
			}
			if ref != tt.wantRef {
				t.Errorf("ref = %v, want %v", ref, tt.wantRef)
			}
			if integ != tt.wantInteg {
				t.Errorf("integ = %v, want %v", integ, tt.wantInteg)
			}
		})
	}
}

func TestService_Stop(t *testing.T) {
	cfg := DefaultServiceConfig()
	cfg.CacheDir = t.TempDir()
	cfg.CarAvailable = false

	svc, err := NewService(cfg)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	// Start async and stop immediately
	svc.StartAsync()
	time.Sleep(50 * time.Millisecond)
	svc.Stop()
	// Should not panic
}

func TestService_DownloadCAR_Integration(t *testing.T) {
	metaData := sdkmeta.Metadata{
		Version:    1,
		Method:     "raw",
		RootCID:    "bafkreidownloadtestabcd1234abcd1234abcd1234abcd12",
		DataTXID:   "txdownloadtestid123456789012345678901234567890123",
		DataHeight: 10,
		DataSize:   100,
		PoW:        "downloadsalt",
		PoWAlg:     "argon2idlightv1",
	}

	carData := []byte("integration-car-data-content")

	server := setupMockServer(t, metaData, carData)
	defer server.Close()

	cacheDir := filepath.Join(t.TempDir(), "integration_cache")

	cfg := DefaultServiceConfig()
	cfg.Preset = pipeline.SecurityTrusted // use trusted since mock data isn't real CAR v2
	cfg.CacheDir = cacheDir
	cfg.MaxFileSize = 1024 * 1024
	cfg.CarAvailable = true
	cfg.GatewayURLs = []string{server.URL}

	svc, err := NewService(cfg)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	result, err := svc.ProcessMetadataTX("meta-tx")
	if err != nil {
		t.Fatalf("ProcessMetadataTX failed: %v", err)
	}

	if !result.Passed {
		t.Errorf("trusted preset should pass, got failure: %+v", result.Steps)
	}

	if result.CARPath == "" {
		t.Error("CAR should have been downloaded")
	} else {
		// Verify CAR content
		data, err := os.ReadFile(result.CARPath)
		if err != nil {
			t.Fatalf("reading CAR file: %v", err)
		}
		if string(data) != string(carData) {
			t.Errorf("CAR content mismatch: %s vs %s", string(data), string(carData))
		}
	}
}

func TestService_DownloadCAR_FetchError(t *testing.T) {
	// Server that returns 404 for everything
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	cfg := DefaultServiceConfig()
	cfg.Preset = pipeline.SecurityTrusted
	cfg.CacheDir = t.TempDir()
	cfg.MaxFileSize = 1024 * 1024
	cfg.GatewayURLs = []string{server.URL}

	svc, err := NewService(cfg)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	_, err = svc.ProcessMetadataTX("tx-404")
	if err == nil {
		t.Error("ProcessMetadataTX should fail with 404")
	}
}

// Test that the service properly handles the download package interface
func TestService_GetFetcher(t *testing.T) {
	cfg := DefaultServiceConfig()
	svc, _ := NewService(cfg)

	// Can't access fetcher directly, but can verify it exists via ProcessMetadata
	if svc.fetcher == nil {
		t.Error("fetcher should not be nil")
	}
}

// The following test verifies that the download package is properly
// integrated with the service
func TestService_DownloadFetcherIntegration(t *testing.T) {
	// Verify that FetcherConfig is properly wired
	cfg := ServiceConfig{
		Preset:       pipeline.SecurityLight,
		CacheDir:     "/tmp/test-ipfar",
		MaxFileSize:  100 * 1024 * 1024,
		GatewayURLs:  []string{"https://test.arweave.net"},
		CarAvailable: true,
	}

	svc, err := NewService(cfg)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	if svc.fetcher == nil {
		t.Fatal("fetcher is nil")
	}

	gw := svc.fetcher.GetGateway()
	if gw == nil {
		t.Fatal("gateway is nil")
	}

	// Download package should have been initialized
	_ = download.DefaultFetcherConfig() // ensure import is used
}
