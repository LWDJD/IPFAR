// Package download 提供 Arweave 数据下载功能
// 支持通过 Arweave 网关获取交易数据和 CAR 文件
// 规范参考: ipfar-specs/V1/项目规划.md §3
package download

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/lwdjd/IPFAR/internal/log"
)

// GatewayConfig Arweave 网关配置
type GatewayConfig struct {
	// URLs 网关 URL 列表（按优先级排序）
	URLs []string
	// Timeout HTTP 请求超时
	Timeout time.Duration
	// MaxRetries 最大重试次数
	MaxRetries int
	// RetryDelay 重试间隔
	RetryDelay time.Duration
	// UserAgent 自定义 User-Agent
	UserAgent string
}

// DefaultGatewayConfig 返回默认网关配置
func DefaultGatewayConfig() GatewayConfig {
	return GatewayConfig{
		URLs: []string{
			"https://arweave.net",
			"https://ar-io.net",
			"https://gateway.irys.xyz",
		},
		Timeout:    30 * time.Second,
		MaxRetries: 3,
		RetryDelay: 1 * time.Second,
		UserAgent:  "IPFAR/1.0",
	}
}

// Gateway Arweave 网关 HTTP 客户端
type Gateway struct {
	config GatewayConfig
	client *http.Client
	mu     sync.RWMutex

	// 统计
	stats GatewayStats
}

// GatewayStats 网关统计信息
type GatewayStats struct {
	TotalRequests   uint64 `json:"total_requests"`
	TotalSuccess    uint64 `json:"total_success"`
	TotalFailures   uint64 `json:"total_failures"`
	BytesDownloaded uint64 `json:"bytes_downloaded"`
}

// NewGateway 创建新的网关客户端
func NewGateway(config GatewayConfig) *Gateway {
	if len(config.URLs) == 0 {
		config.URLs = DefaultGatewayConfig().URLs
	}
	if config.Timeout <= 0 {
		config.Timeout = 30 * time.Second
	}
	if config.MaxRetries <= 0 {
		config.MaxRetries = 3
	}
	if config.RetryDelay <= 0 {
		config.RetryDelay = 1 * time.Second
	}

	transport := &http.Transport{
		MaxIdleConns:        10,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  false,
	}

	return &Gateway{
		config: config,
		client: &http.Client{
			Timeout:   config.Timeout,
			Transport: transport,
		},
	}
}

// FetchTransaction 获取 Arweave 交易数据（原始字节）
// txID: Arweave 交易 ID（Base64URL）
func (g *Gateway) FetchTransaction(txID string) ([]byte, error) {
	path := fmt.Sprintf("/%s", txID)
	return g.fetch(path)
}

// FetchTransactionData 获取交易关联的数据（从 data 端点）
// txID: Arweave 交易 ID
func (g *Gateway) FetchTransactionData(txID string) ([]byte, error) {
	path := fmt.Sprintf("/%s", txID)
	// Arweave 网关对非浏览器请求直接返回原始数据
	return g.fetch(path)
}

// FetchChunk 获取指定交易的指定偏移量数据块
// txID: 交易 ID
// offset: 数据偏移量（字节）
func (g *Gateway) FetchChunk(txID string, offset int64) ([]byte, error) {
	// 有些网关支持 Range 请求
	path := fmt.Sprintf("/%s", txID)
	return g.fetchWithHeaders(path, map[string]string{
		"Range": fmt.Sprintf("bytes=%d-", offset),
	})
}

// HeadTransaction 获取交易头信息（不下载完整数据）
func (g *Gateway) HeadTransaction(txID string) (http.Header, error) {
	path := fmt.Sprintf("/%s", txID)
	return g.head(path)
}

// FetchBlockByHeight 获取指定高度的区块信息
func (g *Gateway) FetchBlockByHeight(height uint64) ([]byte, error) {
	path := fmt.Sprintf("/block/height/%d", height)
	return g.fetch(path)
}

// Stats 返回网关统计信息
func (g *Gateway) Stats() GatewayStats {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.stats
}

// fetch 执行 HTTP GET 请求，所有网关自动容错
func (g *Gateway) fetch(path string) ([]byte, error) {
	return g.fetchWithHeaders(path, nil)
}

// fetchWithHeaders 带自定义头的 GET 请求
func (g *Gateway) fetchWithHeaders(path string, headers map[string]string) ([]byte, error) {
	var lastErr error

	for attempt := 0; attempt < g.config.MaxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(g.config.RetryDelay)
		}

		// 轮询所有网关
		for _, baseURL := range g.config.URLs {
			url := strings.TrimRight(baseURL, "/") + path

			req, err := http.NewRequest(http.MethodGet, url, nil)
			if err != nil {
				lastErr = fmt.Errorf("创建请求失败: %w", err)
				continue
			}

			req.Header.Set("User-Agent", g.config.UserAgent)
			req.Header.Set("Accept", "*/*")
			for k, v := range headers {
				req.Header.Set(k, v)
			}

			g.mu.Lock()
			g.stats.TotalRequests++
			g.mu.Unlock()

			resp, err := g.client.Do(req)
			if err != nil {
				lastErr = fmt.Errorf("网关 %s 请求失败: %w", baseURL, err)
				log.Debug("下载：网关请求失败: %v", err)
				g.mu.Lock()
				g.stats.TotalFailures++
				g.mu.Unlock()
				continue
			}

			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusPartialContent {
				body, err := io.ReadAll(io.LimitReader(resp.Body, 200*1024*1024)) // 限制 200MB
				resp.Body.Close()
				if err != nil {
					lastErr = fmt.Errorf("读取响应失败: %w", err)
					g.mu.Lock()
					g.stats.TotalFailures++
					g.mu.Unlock()
					continue
				}

				g.mu.Lock()
				g.stats.TotalSuccess++
				g.stats.BytesDownloaded += uint64(len(body))
				g.mu.Unlock()

				return body, nil
			}

			resp.Body.Close()

			// 404 / 410 不再重试此网关
			if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusGone {
				lastErr = fmt.Errorf("网关 %s 返回 %d (交易不存在)", baseURL, resp.StatusCode)
				continue
			}

			// 429 限流，等待后重试
			if resp.StatusCode == http.StatusTooManyRequests {
				log.Debug("下载：网关 %s 限流 (429)，等待重试", baseURL)
				time.Sleep(time.Duration(attempt+1) * g.config.RetryDelay)
				lastErr = fmt.Errorf("网关 %s 限流", baseURL)
				continue
			}

			lastErr = fmt.Errorf("网关 %s 返回 HTTP %d", baseURL, resp.StatusCode)
			g.mu.Lock()
			g.stats.TotalFailures++
			g.mu.Unlock()
		}
	}

	return nil, fmt.Errorf("所有网关请求失败（%d 次尝试）: %w", g.config.MaxRetries, lastErr)
}

// head 执行 HTTP HEAD 请求
func (g *Gateway) head(path string) (http.Header, error) {
	var lastErr error

	for _, baseURL := range g.config.URLs {
		url := strings.TrimRight(baseURL, "/") + path

		req, err := http.NewRequest(http.MethodHead, url, nil)
		if err != nil {
			lastErr = err
			continue
		}
		req.Header.Set("User-Agent", g.config.UserAgent)

		g.mu.Lock()
		g.stats.TotalRequests++
		g.mu.Unlock()

		resp, err := g.client.Do(req)
		if err != nil {
			g.mu.Lock()
			g.stats.TotalFailures++
			g.mu.Unlock()
			lastErr = err
			continue
		}
		resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			g.mu.Lock()
			g.stats.TotalSuccess++
			g.mu.Unlock()
			return resp.Header, nil
		}

		lastErr = fmt.Errorf("HEAD %s: HTTP %d", baseURL, resp.StatusCode)
	}

	return nil, fmt.Errorf("HEAD 请求失败: %w", lastErr)
}
