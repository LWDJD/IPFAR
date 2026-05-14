package download

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// ============================================================
// Gateway 核心单元测试
// 规范参考: ipfar-specs/V1/项目规划.md §3
// ============================================================

func TestGateway_NewGateway_Defaults(t *testing.T) {
	// 空配置应使用默认值
	gw := NewGateway(GatewayConfig{})
	if gw == nil {
		t.Fatal("NewGateway returned nil")
	}
	if len(gw.config.URLs) < 3 {
		t.Errorf("空配置应有至少 3 个默认 URL，实际 %d", len(gw.config.URLs))
	}
	if gw.config.Timeout != 30*time.Second {
		t.Errorf("默认 Timeout = %v, want 30s", gw.config.Timeout)
	}
	if gw.config.MaxRetries != 3 {
		t.Errorf("默认 MaxRetries = %d, want 3", gw.config.MaxRetries)
	}
	if gw.config.RetryDelay != 1*time.Second {
		t.Errorf("默认 RetryDelay = %v, want 1s", gw.config.RetryDelay)
	}
	// UserAgent 没有默认回填（仅 DefaultGatewayConfig() 设置），空配置下为空字符串
	if gw.config.UserAgent != "" {
		t.Logf("UserAgent = %q (空配置下通常为空)", gw.config.UserAgent)
	}
}

func TestGateway_NewGateway_CustomConfig(t *testing.T) {
	gw := NewGateway(GatewayConfig{
		URLs:       []string{"https://custom.example.com"},
		Timeout:    10 * time.Second,
		MaxRetries: 5,
		RetryDelay: 2 * time.Second,
		UserAgent:  "CustomAgent/2.0",
	})
	if gw.config.Timeout != 10*time.Second {
		t.Errorf("Timeout = %v, want 10s", gw.config.Timeout)
	}
	if len(gw.config.URLs) != 1 || gw.config.URLs[0] != "https://custom.example.com" {
		t.Errorf("URLs = %v", gw.config.URLs)
	}
}

func TestGateway_FetchTransaction_OK(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("arweave-transaction-data"))
	}))
	defer server.Close()

	gw := NewGateway(GatewayConfig{
		URLs:       []string{server.URL},
		Timeout:    5 * time.Second,
		MaxRetries: 1,
	})

	data, err := gw.FetchTransaction("test-tx-123")
	if err != nil {
		t.Fatalf("FetchTransaction failed: %v", err)
	}
	if string(data) != "arweave-transaction-data" {
		t.Errorf("data = %q, want %q", string(data), "arweave-transaction-data")
	}
}

func TestGateway_FetchTransaction_NotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	gw := NewGateway(GatewayConfig{
		URLs:       []string{server.URL},
		Timeout:    2 * time.Second,
		MaxRetries: 1,
	})

	_, err := gw.FetchTransaction("nonexistent-tx")
	if err == nil {
		t.Error("FetchTransaction 应对 404 返回错误")
	}
}

func TestGateway_FetchTransaction_Gone(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusGone) // 410
	}))
	defer server.Close()

	gw := NewGateway(GatewayConfig{
		URLs:       []string{server.URL},
		Timeout:    2 * time.Second,
		MaxRetries: 1,
	})

	_, err := gw.FetchTransaction("gone-tx")
	if err == nil {
		t.Error("FetchTransaction 应对 410 返回错误")
	}
}

func TestGateway_FetchTransaction_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	gw := NewGateway(GatewayConfig{
		URLs:       []string{server.URL},
		Timeout:    2 * time.Second,
		MaxRetries: 2,
		RetryDelay: 10 * time.Millisecond,
	})

	_, err := gw.FetchTransaction("tx-500")
	if err == nil {
		t.Error("FetchTransaction 应对 500 返回错误")
	}
}

func TestGateway_FetchTransactionData(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("raw-data-content"))
	}))
	defer server.Close()

	gw := NewGateway(GatewayConfig{
		URLs:       []string{server.URL},
		Timeout:    5 * time.Second,
		MaxRetries: 1,
	})

	data, err := gw.FetchTransactionData("some-data-tx")
	if err != nil {
		t.Fatalf("FetchTransactionData failed: %v", err)
	}
	if string(data) != "raw-data-content" {
		t.Errorf("data = %q", string(data))
	}
}

func TestGateway_FetchChunk_RangeRequest(t *testing.T) {
	var receivedRange string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedRange = r.Header.Get("Range")
		w.WriteHeader(http.StatusPartialContent)
		w.Write([]byte("chunk-data"))
	}))
	defer server.Close()

	gw := NewGateway(GatewayConfig{
		URLs:       []string{server.URL},
		Timeout:    5 * time.Second,
		MaxRetries: 1,
	})

	data, err := gw.FetchChunk("tx-chunk", 1024)
	if err != nil {
		t.Fatalf("FetchChunk failed: %v", err)
	}
	if string(data) != "chunk-data" {
		t.Errorf("data = %q", string(data))
	}
	if !strings.HasPrefix(receivedRange, "bytes=1024-") {
		t.Errorf("Range header = %q, want bytes=1024-...", receivedRange)
	}
}

func TestGateway_HeadTransaction_OK(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Errorf("expected HEAD, got %s", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", "12345")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	gw := NewGateway(GatewayConfig{
		URLs:       []string{server.URL},
		Timeout:    5 * time.Second,
		MaxRetries: 1,
	})

	headers, err := gw.HeadTransaction("head-tx")
	if err != nil {
		t.Fatalf("HeadTransaction failed: %v", err)
	}
	if headers.Get("Content-Length") != "12345" {
		t.Errorf("Content-Length = %q, want 12345", headers.Get("Content-Length"))
	}
}

func TestGateway_HeadTransaction_NotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	gw := NewGateway(GatewayConfig{
		URLs:       []string{server.URL},
		Timeout:    2 * time.Second,
		MaxRetries: 1,
	})

	_, err := gw.HeadTransaction("no-such-tx")
	if err == nil {
		t.Error("HeadTransaction 应对 404 返回错误")
	}
}

func TestGateway_FetchBlockByHeight(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/block/height/") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"height":1000}`))
	}))
	defer server.Close()

	gw := NewGateway(GatewayConfig{
		URLs:       []string{server.URL},
		Timeout:    5 * time.Second,
		MaxRetries: 1,
	})

	data, err := gw.FetchBlockByHeight(1000)
	if err != nil {
		t.Fatalf("FetchBlockByHeight failed: %v", err)
	}
	if !strings.Contains(string(data), "1000") {
		t.Errorf("block data = %q", string(data))
	}
}

func TestGateway_StatsAfterRequests(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))
	defer server.Close()

	gw := NewGateway(GatewayConfig{
		URLs:       []string{server.URL},
		Timeout:    5 * time.Second,
		MaxRetries: 1,
	})

	// 初始状态
	stats := gw.Stats()
	if stats.TotalRequests != 0 || stats.TotalSuccess != 0 || stats.TotalFailures != 0 {
		t.Error("初始统计应全部为零")
	}

	// 执行一次成功的请求
	_, err := gw.FetchTransaction("tx")
	if err != nil {
		t.Fatalf("FetchTransaction failed: %v", err)
	}

	stats = gw.Stats()
	if stats.TotalRequests == 0 {
		t.Error("TotalRequests 应 > 0")
	}
	if stats.TotalSuccess == 0 {
		t.Error("TotalSuccess 应 > 0")
	}
	if stats.BytesDownloaded == 0 {
		t.Error("BytesDownloaded 应 > 0")
	}
}

func TestGateway_TooManyRequests_Retry(t *testing.T) {
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		if callCount <= 2 {
			w.WriteHeader(http.StatusTooManyRequests) // 429
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("finally-ok"))
	}))
	defer server.Close()

	gw := NewGateway(GatewayConfig{
		URLs:       []string{server.URL},
		Timeout:    5 * time.Second,
		MaxRetries: 3,
		RetryDelay: 10 * time.Millisecond,
	})

	data, err := gw.FetchTransaction("rate-limited-tx")
	if err != nil {
		t.Fatalf("FetchTransaction should eventually succeed: %v", err)
	}
	if string(data) != "finally-ok" {
		t.Errorf("data = %q", string(data))
	}
	if callCount < 3 {
		t.Errorf("expected at least 3 calls (2x 429 + 1x 200), got %d", callCount)
	}
}

func TestGateway_MultiURL_Fallback(t *testing.T) {
	// server1 始终返回 500
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server1.Close()

	// server2 正常响应
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("from-server2"))
	}))
	defer server2.Close()

	gw := NewGateway(GatewayConfig{
		URLs:       []string{server1.URL, server2.URL},
		Timeout:    5 * time.Second,
		MaxRetries: 1,
		RetryDelay: 10 * time.Millisecond,
	})

	data, err := gw.FetchTransaction("fallback-tx")
	if err != nil {
		t.Fatalf("FetchTransaction should fallback to server2: %v", err)
	}
	if string(data) != "from-server2" {
		t.Errorf("data = %q, want from-server2", string(data))
	}

	// 统计应显示一次成功（server2）
	stats := gw.Stats()
	if stats.TotalSuccess == 0 {
		t.Error("应有成功记录")
	}
}

func TestGateway_MultiURL_AllFail(t *testing.T) {
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server1.Close()

	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server2.Close()

	gw := NewGateway(GatewayConfig{
		URLs:       []string{server1.URL, server2.URL},
		Timeout:    2 * time.Second,
		MaxRetries: 1,
		RetryDelay: 10 * time.Millisecond,
	})

	_, err := gw.FetchTransaction("all-fail-tx")
	if err == nil {
		t.Error("所有网关都失败时应返回错误")
	}
	if !strings.Contains(err.Error(), "所有网关请求失败") {
		t.Errorf("错误信息应包含'所有网关请求失败': %v", err)
	}
}

func TestGateway_UserAgent(t *testing.T) {
	var receivedUA string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedUA = r.Header.Get("User-Agent")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))
	defer server.Close()

	gw := NewGateway(GatewayConfig{
		URLs:      []string{server.URL},
		Timeout:   5 * time.Second,
		UserAgent: "TestAgent/3.0",
	})

	_, err := gw.FetchTransaction("ua-test")
	if err != nil {
		t.Fatalf("FetchTransaction failed: %v", err)
	}
	if receivedUA != "TestAgent/3.0" {
		t.Errorf("User-Agent = %q, want TestAgent/3.0", receivedUA)
	}
}

func TestDefaultGatewayConfig(t *testing.T) {
	cfg := DefaultGatewayConfig()

	if len(cfg.URLs) < 3 {
		t.Errorf("默认应有至少 3 个网关 URL，实际 %d", len(cfg.URLs))
	}

	// 验证包含已知的公共网关
	knownGateways := map[string]bool{
		"https://arweave.net":       false,
		"https://ar-io.net":         false,
		"https://gateway.irys.xyz": false,
	}
	for _, url := range cfg.URLs {
		if _, ok := knownGateways[url]; ok {
			knownGateways[url] = true
		}
	}
	for url, found := range knownGateways {
		if !found {
			t.Errorf("缺少已知公共网关: %s", url)
		}
	}

	if cfg.Timeout != 30*time.Second {
		t.Errorf("默认 Timeout = %v, want 30s", cfg.Timeout)
	}
	if cfg.MaxRetries != 3 {
		t.Errorf("默认 MaxRetries = %d, want 3", cfg.MaxRetries)
	}
}

func TestGateway_FetchChunk_CustomHeaders(t *testing.T) {
	var receivedHeaders http.Header
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedHeaders = r.Header
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))
	defer server.Close()

	gw := NewGateway(GatewayConfig{
		URLs:       []string{server.URL},
		Timeout:    5 * time.Second,
		MaxRetries: 1,
	})

	_, err := gw.FetchChunk("custom-headers", 500)
	if err != nil {
		t.Fatalf("FetchChunk failed: %v", err)
	}
	if receivedHeaders.Get("Range") != "bytes=500-" {
		t.Errorf("Range = %q, want bytes=500-", receivedHeaders.Get("Range"))
	}
	if receivedHeaders.Get("Accept") != "*/*" {
		t.Errorf("Accept = %q, want */*", receivedHeaders.Get("Accept"))
	}
}

// 确保 net/http 正确导入
var _ = httptest.NewServer
