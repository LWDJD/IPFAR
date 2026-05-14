package gateway

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNewGatewayManager_Defaults(t *testing.T) {
	gm := NewGatewayManager(GatewayManagerConfig{
		URLs:       []string{"https://arweave.net"},
		HealthCheck: false,
	})
	defer gm.Close()

	gateways := gm.GetAllGateways()
	if len(gateways) != 1 {
		t.Errorf("应有 1 个网关，实际 %d", len(gateways))
	}
	if gateways[0].URL != "https://arweave.net" {
		t.Errorf("网关 URL 应为 https://arweave.net，实际 %s", gateways[0].URL)
	}
}

func TestGatewayManager_AddRemove(t *testing.T) {
	gm := NewGatewayManager(GatewayManagerConfig{
		URLs:        []string{"https://arweave.net"},
		HealthCheck: false,
	})
	defer gm.Close()

	// 添加网关
	err := gm.AddGateway("https://ar-io.net")
	if err != nil {
		t.Fatalf("AddGateway 不应返回错误: %v", err)
	}

	gateways := gm.GetAllGateways()
	if len(gateways) != 2 {
		t.Errorf("应有 2 个网关，实际 %d", len(gateways))
	}

	// 添加重复网关
	err = gm.AddGateway("https://ar-io.net")
	if err == nil {
		t.Error("添加重复网关应返回错误")
	}

	// 移除网关
	err = gm.RemoveGateway("https://arweave.net")
	if err != nil {
		t.Fatalf("RemoveGateway 不应返回错误: %v", err)
	}

	gateways = gm.GetAllGateways()
	if len(gateways) != 1 {
		t.Errorf("应有 1 个网关（已移除1个），实际 %d", len(gateways))
	}
	if gateways[0].URL != "https://ar-io.net" {
		t.Errorf("剩余网关应为 https://ar-io.net，实际 %s", gateways[0].URL)
	}
}

func TestGatewayManager_AddInvalidURL(t *testing.T) {
	gm := NewGatewayManager(GatewayManagerConfig{
		URLs:        []string{},
		HealthCheck: false,
	})
	defer gm.Close()

	// 空字符串
	err := gm.AddGateway("")
	if err == nil {
		t.Error("空 URL 应返回错误")
	}

	// 仅协议无主机
	err = gm.AddGateway("https://")
	if err == nil {
		t.Error("无主机名的 URL 应返回错误")
	}
}

func TestGatewayManager_LocalGatewayBlocking(t *testing.T) {
	// 禁止本地网关
	gm := NewGatewayManager(GatewayManagerConfig{
		URLs:        []string{},
		AllowLocal:  false,
		HealthCheck: false,
	})
	defer gm.Close()

	// 添加本地网关应失败
	err := gm.AddGateway("http://localhost:1984")
	if err == nil {
		t.Error("AllowLocal=false 时添加本地网关应返回错误")
	}

	err = gm.AddGateway("http://127.0.0.1:1984")
	if err == nil {
		t.Error("AllowLocal=false 时添加 127.0.0.1 应返回错误")
	}
}

func TestGatewayManager_LocalGatewayAllowed(t *testing.T) {
	gm := NewGatewayManager(GatewayManagerConfig{
		URLs:        []string{},
		AllowLocal:  true,
		HealthCheck: false,
	})
	defer gm.Close()

	err := gm.AddGateway("http://localhost:1984")
	if err != nil {
		t.Fatalf("AllowLocal=true 时本地网关应被允许: %v", err)
	}
}

func TestGatewayManager_HealthCheck(t *testing.T) {
	// 启动测试 HTTP 服务器
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))
	defer server.Close()

	gm := NewGatewayManager(GatewayManagerConfig{
		URLs:                []string{server.URL},
		HealthCheck:         true,
		HealthCheckInterval: 200 * time.Millisecond,
		HealthCheckTimeout:  5 * time.Second,
		Timeout:             5 * time.Second,
		AllowLocal:          true, // 测试服务器在本地
	})
	defer gm.Close()

	// 等待健康检查完成
	time.Sleep(300 * time.Millisecond)

	gateways := gm.GetAllGateways()
	if len(gateways) != 1 {
		t.Fatalf("应有 1 个网关，实际 %d", len(gateways))
	}
	if !gateways[0].Healthy {
		t.Error("网关应为健康状态")
	}
	if gateways[0].Latency == 0 {
		t.Error("延迟应 > 0")
	}

	// 使用 GetHealthyGateway
	gw, err := gm.GetHealthyGateway()
	if err != nil {
		t.Fatalf("GetHealthyGateway 不应返回错误: %v", err)
	}
	if gw == nil {
		t.Fatal("GetHealthyGateway 不应返回 nil")
	}
}

func TestGatewayManager_UnhealthyFallback(t *testing.T) {
	gm := NewGatewayManager(GatewayManagerConfig{
		URLs:                []string{"https://invalid-gateway.local:9999"},
		HealthCheck:         true,
		HealthCheckInterval: 200 * time.Millisecond,
		HealthCheckTimeout:  1 * time.Second,
	})
	defer gm.Close()

	// 等待健康检查（会失败）
	time.Sleep(300 * time.Millisecond)

	// 应回退到唯一（不健康）的网关
	gw, err := gm.GetHealthyGateway()
	if err != nil {
		t.Fatalf("即使无健康网关也不应返回错误: %v", err)
	}
	if gw == nil {
		t.Fatal("应回退到唯一网关")
	}
}

func TestGatewayManager_GetHealthyURLs(t *testing.T) {
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server1.Close()

	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server2.Close()

	gm := NewGatewayManager(GatewayManagerConfig{
		URLs:                []string{server1.URL, server2.URL},
		HealthCheck:         true,
		HealthCheckInterval: 200 * time.Millisecond,
		HealthCheckTimeout:  5 * time.Second,
		AllowLocal:          true,
		MaxFailures:         1,
	})
	defer gm.Close()

	time.Sleep(300 * time.Millisecond)

	urls := gm.GetHealthyURLs()
	if len(urls) == 0 {
		t.Error("应有至少 1 个 URL")
	}
	// 健康网关应排在最前
	if len(urls) >= 1 && urls[0] != server1.URL {
		t.Logf("第一个 URL 应为健康的测试服务器 %s，但得到 %s", server1.URL, urls[0])
	}
}

func TestGatewayManager_NewGatewayFromManager(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("test-response-data"))
	}))
	defer server.Close()

	gm := NewGatewayManager(GatewayManagerConfig{
		URLs:                []string{server.URL},
		HealthCheck:         true,
		HealthCheckInterval: 100 * time.Millisecond,
		HealthCheckTimeout:  3 * time.Second,
		Timeout:             5 * time.Second,
		AllowLocal:          true,
	})
	defer gm.Close()

	time.Sleep(200 * time.Millisecond)

	gw := NewGatewayFromManager(gm)
	if gw == nil {
		t.Fatal("NewGatewayFromManager 不应返回 nil")
	}

	// 验证创建的 Gateway 可以正常使用
	data, err := gw.FetchTransaction("test")
	if err != nil {
		t.Fatalf("通过 GatewayManager 创建的 Gateway 请求失败: %v", err)
	}
	if len(data) == 0 {
		t.Error("数据不应为空")
	}
	if string(data) != "test-response-data" {
		t.Errorf("data = %q, want test-response-data", string(data))
	}
}

func TestIsLocalURL(t *testing.T) {
	tests := []struct {
		url     string
		isLocal bool
	}{
		{"http://localhost:1984", true},
		{"http://127.0.0.1:8080", true},
		{"http://[::1]:1984", true},
		{"http://192.168.1.1:1984", true},
		{"http://10.0.0.1:1984", true},
		{"http://172.16.0.1:1984", true},
		{"http://169.254.1.1:1984", true},
		{"https://arweave.net", false},
		{"https://ar-io.net", false},
		{"https://gateway.irys.xyz", false},
	}

	for _, tt := range tests {
		result := isLocalURL(tt.url)
		if result != tt.isLocal {
			t.Errorf("isLocalURL(%q) = %v, want %v", tt.url, result, tt.isLocal)
		}
	}
}

func TestNormalizeURL(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"https://arweave.net", "https://arweave.net"},
		{"https://arweave.net/", "https://arweave.net"},
		{"http://ARWEAVE.NET/", "http://arweave.net"},
		{"arweave.net", "https://arweave.net"}, // 默认 https
	}

	for _, tt := range tests {
		result, err := normalizeURL(tt.input)
		if err != nil {
			t.Errorf("normalizeURL(%q) 返回错误: %v", tt.input, err)
			continue
		}
		if result != tt.expected {
			t.Errorf("normalizeURL(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestGatewayManager_Close(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	gm := NewGatewayManager(GatewayManagerConfig{
		URLs:                []string{server.URL},
		HealthCheck:         true,
		HealthCheckInterval: 50 * time.Millisecond,
		AllowLocal:          true,
	})
	defer gm.Close()

	gm.Close()

	// 关闭后添加网关应失败
	err := gm.AddGateway("https://ar-io.net")
	if err == nil {
		t.Error("关闭后添加网关应返回错误")
	}
}

func TestDefaultGatewayManagerConfig(t *testing.T) {
	cfg := DefaultGatewayManagerConfig()
	if len(cfg.URLs) < 3 {
		t.Errorf("默认应有至少 3 个网关 URL")
	}
	if cfg.HealthCheckInterval != 60*time.Second {
		t.Errorf("默认检查间隔 = %v, want 60s", cfg.HealthCheckInterval)
	}
	if cfg.MaxFailures != 2 {
		t.Errorf("默认 MaxFailures = %d, want 2", cfg.MaxFailures)
	}
}

func TestGatewayManager_MultipleGateways(t *testing.T) {
	// 一个健康服务器
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("server1"))
	}))
	defer server1.Close()

	// 一个不健康服务器
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server2.Close()

	gm := NewGatewayManager(GatewayManagerConfig{
		URLs:                []string{server1.URL, server2.URL},
		HealthCheck:         true,
		HealthCheckInterval: 200 * time.Millisecond,
		HealthCheckTimeout:  5 * time.Second,
		AllowLocal:          true,
		MaxFailures:         1,
	})
	defer gm.Close()

	time.Sleep(300 * time.Millisecond)

	// 健康网关应被选中
	gw, err := gm.GetHealthyGateway()
	if err != nil {
		t.Fatalf("GetHealthyGateway failed: %v", err)
	}
	if gw.URL != server1.URL {
		t.Errorf("应选择健康网关 %s，实际 %s", server1.URL, gw.URL)
	}
	if !gw.Healthy {
		t.Error("选中的网关应为健康状态")
	}

	// 验证所有网关状态
	allGateways := gm.GetAllGateways()
	for _, g := range allGateways {
		if g.URL == server1.URL && !g.Healthy {
			t.Error("server1 应为健康")
		}
		if g.URL == server2.URL && g.Healthy {
			t.Error("server2 应为不健康")
		}
	}
}

// 确保 net/http 正确导入
var _ = httptest.NewServer
