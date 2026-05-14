// Package gateway 提供 Arweave 网关管理与健康检查功能
// 支持多网关负载均衡、自动故障转移和健康检查
// 规范参考: ipfar-specs/V1/项目规划.md §3
package gateway

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/lwdjd/IPFAR/internal/download"
	"github.com/lwdjd/IPFAR/internal/log"
)

// GatewayInfo 单个 Arweave 网关的状态信息
type GatewayInfo struct {
	// URL 网关地址（例如 https://arweave.net）
	URL string `json:"url"`
	// Healthy 当前是否健康
	Healthy bool `json:"healthy"`
	// Latency 最后一次健康检查的延迟
	Latency time.Duration `json:"latency"`
	// LastCheck 最后一次健康检查的时间
	LastCheck time.Time `json:"last_check"`
	// Failures 连续失败次数
	Failures int `json:"failures"`
	// IsLocal 是否为本地网关（localhost / 内网地址）
	IsLocal bool `json:"is_local"`
}

// GatewayManagerConfig 网关管理器配置
type GatewayManagerConfig struct {
	// URLs 初始网关 URL 列表
	URLs []string
	// AllowLocal 是否允许本地网关（默认 false）
	AllowLocal bool
	// HealthCheck 是否启用健康检查（默认 true）
	HealthCheck bool
	// HealthCheckInterval 健康检查间隔（默认 60s）
	HealthCheckInterval time.Duration
	// HealthCheckTimeout 单次健康检查超时（默认 10s）
	HealthCheckTimeout time.Duration
	// Timeout HTTP 请求超时（传递给 download.Gateway，默认 30s）
	Timeout time.Duration
	// MaxRetries 最大重试次数（传递给 download.Gateway，默认 3）
	MaxRetries int
	// MaxFailures 连续失败多少次后标记为不健康（默认 2）
	MaxFailures int
}

// DefaultGatewayManagerConfig 返回默认网关管理器配置
func DefaultGatewayManagerConfig() GatewayManagerConfig {
	return GatewayManagerConfig{
		URLs: []string{
			"https://arweave.net",
			"https://ar-io.net",
			"https://gateway.irys.xyz",
		},
		AllowLocal:          false,
		HealthCheck:         true,
		HealthCheckInterval: 60 * time.Second,
		HealthCheckTimeout:  10 * time.Second,
		Timeout:             30 * time.Second,
		MaxRetries:          3,
		MaxFailures:         2,
	}
}

// GatewayManager 管理多个 Arweave 网关
// 负责健康检查、故障转移和网关列表维护
type GatewayManager struct {
	config GatewayManagerConfig

	mu       sync.RWMutex
	gateways map[string]*GatewayInfo // key = normalized URL

	httpClient *http.Client
	stopCh     chan struct{}
	stopped    bool
}

// NewGatewayManager 创建新的网关管理器
func NewGatewayManager(config GatewayManagerConfig) *GatewayManager {
	if len(config.URLs) == 0 {
		config.URLs = DefaultGatewayManagerConfig().URLs
	}
	if config.HealthCheckInterval <= 0 {
		config.HealthCheckInterval = 60 * time.Second
	}
	if config.HealthCheckTimeout <= 0 {
		config.HealthCheckTimeout = 10 * time.Second
	}
	if config.Timeout <= 0 {
		config.Timeout = 30 * time.Second
	}
	if config.MaxRetries <= 0 {
		config.MaxRetries = 3
	}
	if config.MaxFailures <= 0 {
		config.MaxFailures = 2
	}

	transport := &http.Transport{
		MaxIdleConns:    10,
		IdleConnTimeout: 90 * time.Second,
	}

	gm := &GatewayManager{
		config:   config,
		gateways: make(map[string]*GatewayInfo),
		httpClient: &http.Client{
			Timeout:   config.HealthCheckTimeout,
			Transport: transport,
		},
		stopCh: make(chan struct{}),
	}

	// 初始化网关列表
	for _, rawURL := range config.URLs {
		if err := gm.addGatewayInternal(rawURL); err != nil {
			log.Debug("网关：初始化网关 %s 失败: %v", rawURL, err)
		}
	}

	// 启动健康检查
	if config.HealthCheck {
		// 首次检查立即执行
		gm.checkAllGateways()
		go gm.healthCheckLoop()
	}

	return gm
}

// AddGateway 添加一个网关
func (gm *GatewayManager) AddGateway(rawURL string) error {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	if gm.stopped {
		return fmt.Errorf("网关管理器已关闭")
	}

	return gm.addGatewayInternal(rawURL)
}

// addGatewayInternal 内部添加网关（调用方必须持有锁）
func (gm *GatewayManager) addGatewayInternal(rawURL string) error {
	normalized, err := normalizeURL(rawURL)
	if err != nil {
		return fmt.Errorf("无效的网关 URL %q: %w", rawURL, err)
	}

	// 检查是否已存在
	if _, exists := gm.gateways[normalized]; exists {
		return fmt.Errorf("网关 %s 已存在", normalized)
	}

	// 检查是否允许本地网关
	isLocal := isLocalURL(normalized)
	if isLocal && !gm.config.AllowLocal {
		return fmt.Errorf("不允许本地网关（AllowLocal=false）: %s", normalized)
	}

	gm.gateways[normalized] = &GatewayInfo{
		URL:     normalized,
		Healthy: true, // 初始假设健康
		IsLocal: isLocal,
	}

	log.Debug("网关：已添加 %s (本地=%v)", normalized, isLocal)
	return nil
}

// RemoveGateway 移除一个网关
func (gm *GatewayManager) RemoveGateway(rawURL string) error {
	normalized, err := normalizeURL(rawURL)
	if err != nil {
		return fmt.Errorf("无效的网关 URL %q: %w", rawURL, err)
	}

	gm.mu.Lock()
	defer gm.mu.Unlock()

	if gm.stopped {
		return fmt.Errorf("网关管理器已关闭")
	}

	if _, exists := gm.gateways[normalized]; !exists {
		return fmt.Errorf("网关 %s 不存在", normalized)
	}

	delete(gm.gateways, normalized)
	log.Debug("网关：已移除 %s", normalized)
	return nil
}

// GetAllGateways 返回所有网关的副本
func (gm *GatewayManager) GetAllGateways() []*GatewayInfo {
	gm.mu.RLock()
	defer gm.mu.RUnlock()

	result := make([]*GatewayInfo, 0, len(gm.gateways))
	for _, gw := range gm.gateways {
		// 返回副本以避免竞态
		copy := *gw
		result = append(result, &copy)
	}
	return result
}

// GetHealthyGateway 返回最佳的健康网关
// 优先选择延迟最低的健康网关。如果没有健康网关，回退到任意网关。
func (gm *GatewayManager) GetHealthyGateway() (*GatewayInfo, error) {
	gm.mu.RLock()
	defer gm.mu.RUnlock()

	if len(gm.gateways) == 0 {
		return nil, fmt.Errorf("没有可用网关")
	}

	// 查找延迟最低的健康网关
	var best *GatewayInfo
	for _, gw := range gm.gateways {
		if gw.Healthy {
			if best == nil || gw.Latency < best.Latency {
				best = gw
			}
		}
	}

	// 如果没有健康网关，回退到任意一个
	if best == nil {
		for _, gw := range gm.gateways {
			best = gw
			break
		}
		log.Debug("网关：无健康网关，回退到 %s", best.URL)
	}

	copy := *best
	return &copy, nil
}

// GetHealthyURLs 返回所有健康网关的 URL 列表
func (gm *GatewayManager) GetHealthyURLs() []string {
	gm.mu.RLock()
	defer gm.mu.RUnlock()

	urls := make([]string, 0, len(gm.gateways))
	// 先加入健康网关
	for _, gw := range gm.gateways {
		if gw.Healthy {
			urls = append(urls, gw.URL)
		}
	}
	// 再加入不健康网关作为后备
	for _, gw := range gm.gateways {
		if !gw.Healthy {
			urls = append(urls, gw.URL)
		}
	}
	return urls
}

// Close 关闭网关管理器，停止健康检查
func (gm *GatewayManager) Close() {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	if !gm.stopped {
		gm.stopped = true
		close(gm.stopCh)
	}
}

// healthCheckLoop 定期健康检查循环
func (gm *GatewayManager) healthCheckLoop() {
	ticker := time.NewTicker(gm.config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-gm.stopCh:
			return
		case <-ticker.C:
			gm.checkAllGateways()
		}
	}
}

// checkAllGateways 检查所有网关的健康状态
func (gm *GatewayManager) checkAllGateways() {
	gm.mu.RLock()
	urls := make([]string, 0, len(gm.gateways))
	for url := range gm.gateways {
		urls = append(urls, url)
	}
	gm.mu.RUnlock()

	for _, gwURL := range urls {
		healthy, latency := gm.checkGateway(gwURL)

		gm.mu.Lock()
		if gw, exists := gm.gateways[gwURL]; exists {
			gw.LastCheck = time.Now()
			gw.Latency = latency
			gw.Healthy = healthy
			if healthy {
				gw.Failures = 0
			} else {
				gw.Failures++
				if gw.Failures >= gm.config.MaxFailures {
					gw.Healthy = false
					log.Debug("网关：%s 连续失败 %d 次，标记为不健康", gwURL, gw.Failures)
				}
			}
		}
		gm.mu.Unlock()
	}
}

// checkGateway 检查单个网关的健康状态
// 返回 (healthy bool, latency time.Duration)
func (gm *GatewayManager) checkGateway(gwURL string) (bool, time.Duration) {
	start := time.Now()

	// 使用 HEAD 请求检查网关 /arweave/info 端点或根路径
	req, err := http.NewRequest(http.MethodGet, gwURL+"/", nil)
	if err != nil {
		return false, time.Since(start)
	}
	req.Header.Set("User-Agent", "IPFAR/1.0 GatewayHealthCheck")

	resp, err := gm.httpClient.Do(req)
	if err != nil {
		return false, time.Since(start)
	}
	defer resp.Body.Close()

	latency := time.Since(start)

	// 任何非 5xx 响应视为健康（包括 404，说明网关可达）
	return resp.StatusCode < 500, latency
}

// isLocalURL 判断 URL 是否为本地地址
func isLocalURL(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}

	host := u.Hostname()

	// localhost
	if host == "localhost" || host == "127.0.0.1" || host == "::1" {
		return true
	}

	// 私有网络地址
	ip := net.ParseIP(host)
	if ip != nil {
		if ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
			return true
		}
	}

	return false
}

// normalizeURL 规范化 URL（去重、标准化格式）
func normalizeURL(rawURL string) (string, error) {
	// 如果没有 scheme，url.Parse 会把它当作相对路径
	// 先尝试添加 scheme
	if !strings.Contains(rawURL, "://") {
		rawURL = "https://" + rawURL
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	if u.Host == "" {
		return "", fmt.Errorf("URL 缺少主机名: %s", rawURL)
	}

	// 去除尾部斜杠
	normalized := fmt.Sprintf("%s://%s%s", u.Scheme, u.Host, strings.TrimRight(u.Path, "/"))
	return strings.ToLower(normalized), nil
}

// NewGatewayFromManager 从 GatewayManager 创建 download.Gateway
// 使用健康网关列表，支持自动故障转移
func NewGatewayFromManager(gm *GatewayManager) *download.Gateway {
	urls := gm.GetHealthyURLs()
	return download.NewGateway(download.GatewayConfig{
		URLs:       urls,
		Timeout:    gm.config.Timeout,
		MaxRetries: gm.config.MaxRetries,
		RetryDelay: 1 * time.Second,
	})
}
