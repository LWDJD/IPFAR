package download

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
)

// ============================================================
// Gateway Tests
// ============================================================

func TestNewGateway(t *testing.T) {
	cfg := DefaultGatewayConfig()
	gw := NewGateway(cfg)
	if gw == nil {
		t.Fatal("NewGateway returned nil")
	}
	if gw.config.Timeout != 30*time.Second {
		t.Errorf("Timeout = %v, want 30s", gw.config.Timeout)
	}
	if len(gw.config.URLs) == 0 {
		t.Error("URLs should not be empty")
	}
}

func TestGateway_WithMockServer(t *testing.T) {
	// 创建模拟网关服务器
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/tx-valid"):
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			meta := sdkmeta.Metadata{
				Version:    1,
				Method:     "raw",
				RootCID:    "bafkreiabcdef",
				DataTXID:   "tx-car-123",
				DataHeight: 1000,
				DataSize:   1024,
				PoW:        "salt123",
				PoWAlg:     "argon2id-light-v1",
			}
			json.NewEncoder(w).Encode(meta)
		case strings.Contains(r.URL.Path, "/tx-car-123"):
			w.Header().Set("Content-Type", "application/vnd.ipld.car")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("mock-car-data"))
		case strings.Contains(r.URL.Path, "/tx-missing"):
			w.WriteHeader(http.StatusNotFound)
		case strings.Contains(r.URL.Path, "/tx-error"):
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	cfg := GatewayConfig{
		URLs:       []string{server.URL},
		Timeout:    5 * time.Second,
		MaxRetries: 1,
		RetryDelay: 10 * time.Millisecond,
	}

	gw := NewGateway(cfg)

	// 测试成功获取
	data, err := gw.FetchTransaction("tx-valid")
	if err != nil {
		t.Fatalf("FetchTransaction failed: %v", err)
	}
	if len(data) == 0 {
		t.Error("FetchTransaction returned empty data")
	}

	// 测试 404
	_, err = gw.FetchTransaction("tx-missing")
	if err == nil {
		t.Error("FetchTransaction should fail for missing tx")
	}

	// 验证统计
	stats := gw.Stats()
	if stats.TotalRequests == 0 {
		t.Error("Stats should show some requests")
	}
}

func TestGateway_Stats(t *testing.T) {
	cfg := DefaultGatewayConfig()
	gw := NewGateway(cfg)

	stats := gw.Stats()
	if stats.TotalRequests != 0 {
		t.Error("Initial stats should be zero")
	}
}

// ============================================================
// Fetcher Tests
// ============================================================

func TestNewFetcher(t *testing.T) {
	f := NewFetcher(DefaultFetcherConfig())
	if f == nil {
		t.Fatal("NewFetcher returned nil")
	}
	if f.gateway == nil {
		t.Fatal("Fetcher gateway should not be nil")
	}
}

func TestFetcher_FetchMetadataFromJSON(t *testing.T) {
	f := NewFetcher(DefaultFetcherConfig())

	validJSON := `{
		"version": 1,
		"method": "raw",
		"root_cid": "bafkreiabcdefghijklmnopqrstuvwxyz1234567890",
		"data_txid": "tx-valid-data-txid-1234567890123456789012345678901234567890",
		"data_height": 1000,
		"data_size": 1024,
		"pow": "salt123",
		"pow_alg": "argon2id-light-v1"
	}`

	meta, err := f.FetchMetadataFromJSON([]byte(validJSON))
	if err != nil {
		t.Fatalf("FetchMetadataFromJSON failed: %v", err)
	}
	if meta.RootCID != "bafkreiabcdefghijklmnopqrstuvwxyz1234567890" {
		t.Errorf("RootCID = %s", meta.RootCID)
	}
	if meta.DataTXID != "tx-valid-data-txid-1234567890123456789012345678901234567890" {
		t.Errorf("DataTXID = %s", meta.DataTXID)
	}
}

func TestFetcher_FetchMetadataFromJSON_Invalid(t *testing.T) {
	f := NewFetcher(DefaultFetcherConfig())

	_, err := f.FetchMetadataFromJSON([]byte(`{"invalid": true}`))
	if err == nil {
		t.Error("Should fail for invalid JSON metadata")
	}
}

func TestFetcher_FetchMetadataFromBase64(t *testing.T) {
	f := NewFetcher(DefaultFetcherConfig())

	// 构造一个合法的元数据并编码
	meta := &sdkmeta.Metadata{
		Version:    1,
		Method:     "raw",
		RootCID:    "bafkreiabcdefghijklmnopqrstuvwxyz1234567890",
		DataTXID:   "tx-valid-data-txid-1234567890123456789012345678901234567890",
		DataHeight: 1000,
		DataSize:   1024,
		PoW:        "salt123",
		PoWAlg:     "argon2id-light-v1",
	}
	encoded, err := meta.ToBase64URL()
	if err != nil {
		t.Fatalf("ToBase64URL failed: %v", err)
	}

	decoded, err := f.FetchMetadataFromBase64(encoded)
	if err != nil {
		t.Fatalf("FetchMetadataFromBase64 failed: %v", err)
	}
	if decoded.RootCID != meta.RootCID {
		t.Errorf("RootCID mismatch: %s != %s", decoded.RootCID, meta.RootCID)
	}
}

func TestFetcher_FetchMetadataByTXID_WithMockServer(t *testing.T) {
	// 创建模拟网关
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		meta := sdkmeta.Metadata{
			Version:    1,
			Method:     "raw",
			RootCID:    "bafkreitestrootcidabcd1234abcd1234abcd1234abcd1234",
			DataTXID:   "txcartest1234567890123456789012345678901234567890",
			DataHeight: 5000,
			DataSize:   2048,
			PoW:        "testsalt",
			PoWAlg:     "argon2idlightv1",
		}
		jsonBytes, _ := json.Marshal(meta)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(jsonBytes)
	}))
	defer server.Close()

	cfg := FetcherConfig{
		Gateway: NewGateway(GatewayConfig{
			URLs:       []string{server.URL},
			Timeout:    5 * time.Second,
			MaxRetries: 1,
			RetryDelay: 10 * time.Millisecond,
		}),
		CacheDir:    t.TempDir(),
		MaxFileSize: 10 * 1024 * 1024,
	}

	f := NewFetcher(cfg)

	meta, err := f.FetchMetadataByTXID("any-tx-id")
	if err != nil {
		t.Fatalf("FetchMetadataByTXID failed: %v", err)
	}
	if meta.DataHeight != 5000 {
		t.Errorf("DataHeight = %d, want 5000", meta.DataHeight)
	}
}

func TestFetcher_DownloadCAR_WithMockServer(t *testing.T) {
	mockCARData := []byte("mock-car-data-content-12345")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.ipld.car")
		w.WriteHeader(http.StatusOK)
		w.Write(mockCARData)
	}))
	defer server.Close()

	cacheDir := filepath.Join(t.TempDir(), "car_cache")

	cfg := FetcherConfig{
		Gateway: NewGateway(GatewayConfig{
			URLs:       []string{server.URL},
			Timeout:    5 * time.Second,
			MaxRetries: 1,
			RetryDelay: 10 * time.Millisecond,
		}),
		CacheDir:    cacheDir,
		MaxFileSize: 10 * 1024 * 1024,
	}

	f := NewFetcher(cfg)

	meta := &sdkmeta.Metadata{
		RootCID:  "bafkrei-test-root-cid",
		DataTXID: "tx-car-mock",
		DataSize: len(mockCARData),
	}

	cachePath, err := f.DownloadCAR(meta)
	if err != nil {
		t.Fatalf("DownloadCAR failed: %v", err)
	}

	// 验证文件存在且内容正确
	data, err := os.ReadFile(cachePath)
	if err != nil {
		t.Fatalf("读取缓存文件失败: %v", err)
	}
	if string(data) != string(mockCARData) {
		t.Errorf("缓存内容不正确: got %q, want %q", string(data), string(mockCARData))
	}

	// 验证缓存路径格式
	if !strings.Contains(cachePath, "car_cache") {
		t.Errorf("缓存路径应包含 car_cache: %s", cachePath)
	}
}

func TestFetcher_DownloadCAR_CacheHit(t *testing.T) {
	mockCARData := []byte("cached-car-data")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(mockCARData)
	}))
	defer server.Close()

	cacheDir := filepath.Join(t.TempDir(), "car_cache")

	cfg := FetcherConfig{
		Gateway: NewGateway(GatewayConfig{
			URLs:       []string{server.URL},
			Timeout:    5 * time.Second,
			MaxRetries: 1,
			RetryDelay: 10 * time.Millisecond,
		}),
		CacheDir:    cacheDir,
		MaxFileSize: 10 * 1024 * 1024,
	}

	f := NewFetcher(cfg)

	meta := &sdkmeta.Metadata{
		RootCID:  "bafkrei-test-root-cid",
		DataTXID: "tx-car-cached",
		DataSize: len(mockCARData),
	}

	// 第一次下载
	path1, err := f.DownloadCAR(meta)
	if err != nil {
		t.Fatalf("第一次 DownloadCAR 失败: %v", err)
	}

	// 第二次下载（应命中缓存）
	path2, err := f.DownloadCAR(meta)
	if err != nil {
		t.Fatalf("第二次 DownloadCAR 失败: %v", err)
	}

	if path1 != path2 {
		t.Errorf("缓存路径不一致: %s != %s", path1, path2)
	}
}

func TestFetcher_DownloadCAR_BigFile(t *testing.T) {
	cfg := FetcherConfig{
		Gateway:     NewGateway(DefaultGatewayConfig()),
		CacheDir:    t.TempDir(),
		MaxFileSize: 100, // 限制 100 字节
	}

	f := NewFetcher(cfg)

	meta := &sdkmeta.Metadata{
		RootCID:  "bafkrei-test",
		DataTXID: "tx-big",
		DataSize: 1000, // 超过限制
	}

	_, err := f.DownloadCAR(meta)
	if err == nil {
		t.Error("DownloadCAR 应拒绝超过 MaxFileSize 的文件")
	}
}

func TestFetcher_FetchAndDownload(t *testing.T) {
	carData := []byte("full-chain-car-data")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case strings.Contains(path, "/tx-meta"):
			meta := sdkmeta.Metadata{
				Version:    1,
				Method:     "raw",
				RootCID:    "bafkreifullchaintestabcd1234abcd1234abcd1234ab",
				DataTXID:   "txfullchaincar12345678901234567890123456789012345",
				DataHeight: 100,
				DataSize:   len(carData),
				PoW:        "fullchainsalt",
				PoWAlg:     "argon2idlightv1",
			}
			jsonBytes, _ := json.Marshal(meta)
			w.Header().Set("Content-Type", "application/json")
			w.Write(jsonBytes)
		case strings.Contains(path, "/txfullchaincar"):
			w.Header().Set("Content-Type", "application/vnd.ipld.car")
			w.Write(carData)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	cfg := FetcherConfig{
		Gateway: NewGateway(GatewayConfig{
			URLs:       []string{server.URL},
			Timeout:    5 * time.Second,
			MaxRetries: 1,
			RetryDelay: 10 * time.Millisecond,
		}),
		CacheDir:    t.TempDir(),
		MaxFileSize: 10 * 1024 * 1024,
	}

	f := NewFetcher(cfg)

	meta, carPath, err := f.FetchAndDownload("tx-meta")
	if err != nil {
		t.Fatalf("FetchAndDownload failed: %v", err)
	}

	if meta == nil {
		t.Fatal("meta is nil")
	}
	if carPath == "" {
		t.Fatal("carPath is empty")
	}
	if meta.RootCID != "bafkreifullchaintestabcd1234abcd1234abcd1234ab" {
		t.Errorf("RootCID = %s", meta.RootCID)
	}

	// 验证 CAR 文件内容
	data, err := os.ReadFile(carPath)
	if err != nil {
		t.Fatalf("读取 CAR 缓存失败: %v", err)
	}
	if string(data) != string(carData) {
		t.Errorf("CAR 内容不正确")
	}
}

func TestFetcher_GetGateway(t *testing.T) {
	gw := NewGateway(DefaultGatewayConfig())
	cfg := FetcherConfig{
		Gateway: gw,
	}
	f := NewFetcher(cfg)

	if f.GetGateway() != gw {
		t.Error("GetGateway should return the configured gateway")
	}
}

func TestFetcher_ClearCache(t *testing.T) {
	cacheDir := filepath.Join(t.TempDir(), "clear_test_cache")

	// 创建一个文件在缓存目录中
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		t.Fatal(err)
	}
	testFile := filepath.Join(cacheDir, "test.car")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		t.Fatal(err)
	}

	f := NewFetcher(FetcherConfig{
		CacheDir: cacheDir,
	})

	if err := f.ClearCache(); err != nil {
		t.Fatalf("ClearCache failed: %v", err)
	}

	// 验证目录已删除
	if _, err := os.Stat(cacheDir); !os.IsNotExist(err) {
		t.Error("缓存目录应该已被删除")
	}
}

func TestFetcher_DownloadReferenceData(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "data-for-%s", r.URL.Path)
	}))
	defer server.Close()

	cfg := FetcherConfig{
		Gateway: NewGateway(GatewayConfig{
			URLs:       []string{server.URL},
			Timeout:    5 * time.Second,
			MaxRetries: 1,
			RetryDelay: 10 * time.Millisecond,
		}),
		CacheDir:    t.TempDir(),
		MaxFileSize: 10 * 1024 * 1024,
	}

	f := NewFetcher(cfg)

	refMap := sdkmeta.ReferenceMap{
		"ref-tx-1": {Height: 100, CIDs: []string{"cid-1"}},
		"ref-tx-2": {Height: 200, CIDs: []string{"cid-2"}},
	}

	meta := &sdkmeta.Metadata{
		Version:   1,
		Method:    "raw",
		RootCID:   "bafkrei-test",
		DataTXID:  "tx-main",
		DataSize:  100,
		Reference: &refMap,
	}

	refPaths, err := f.DownloadReferenceData(meta)
	if err != nil {
		t.Fatalf("DownloadReferenceData failed: %v", err)
	}

	if len(refPaths) != 2 {
		t.Errorf("expected 2 reference downloads, got %d", len(refPaths))
	}

	for txID, path := range refPaths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Errorf("读取引用文件失败 %s: %v", txID, err)
			continue
		}
		if len(data) == 0 {
			t.Errorf("引用文件 %s 为空", txID)
		}
	}
}

func TestSanitizeFilename(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"normal.car", "normal.car"},
		{"file/with/slash.car", "file_with_slash.car"},
		{"file\\backslash.car", "file_backslash.car"},
		{"file:colon.car", "file_colon.car"},
		{"file*star.car", "file_star.car"},
		{"file?question.car", "file_question.car"},
		{"file\"quote.car", "file_quote.car"},
		{"file<lt.car", "file_lt.car"},
		{"file>gt.car", "file_gt.car"},
		{"file|pipe.car", "file_pipe.car"},
	}

	for _, tt := range tests {
		result := sanitizeFilename(tt.input)
		if result != tt.expected {
			t.Errorf("sanitizeFilename(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}
