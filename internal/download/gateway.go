// Package download 提供 Arweave 数据下载功能
// 支持通过 Arweave 网关获取交易数据和 CAR 文件
// 规范参考: ipfar-specs/V1/项目规划.md §3
package download

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/lwdjd/IPFAR/internal/log"
	"golang.org/x/net/proxy"
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
	// Socks5Proxy SOCKS5 代理地址，如 "127.0.0.1:10808"
	// 为空则不使用代理。会自动读取 SOCKS5_PROXY/socks5_proxy 环境变量
	Socks5Proxy string
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

	// 如果未显式设置代理，从环境变量读取
	if config.Socks5Proxy == "" {
		for _, env := range []string{"SOCKS5_PROXY", "socks5_proxy", "all_proxy", "ALL_PROXY"} {
			if v := os.Getenv(env); v != "" {
				config.Socks5Proxy = v
				break
			}
		}
	}

	transport := &http.Transport{
		MaxIdleConns:        10,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  false,
	}

	// 配置 SOCKS5 代理
	if config.Socks5Proxy != "" {
		proxyAddr := config.Socks5Proxy
		// 去除 socks5:// 前缀（如果有）
		proxyAddr = strings.TrimPrefix(proxyAddr, "socks5://")
		proxyAddr = strings.TrimPrefix(proxyAddr, "socks5h://")

		dialer, err := proxy.SOCKS5("tcp", proxyAddr, nil, proxy.Direct)
		if err != nil {
			log.Warn("SOCKS5 代理配置失败 (%s): %v，将直连", proxyAddr, err)
		} else {
			transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
				return dialer.Dial(network, addr)
			}
			log.Info("已配置 SOCKS5 代理: %s", proxyAddr)
		}
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

// FetchTransactionData 获取交易关联的数据（从 /raw 端点）
// txID: Arweave 交易 ID
func (g *Gateway) FetchTransactionData(txID string) ([]byte, error) {
	// 优先使用 /raw 端点，避免裸端点返回 570 等问题
	path := fmt.Sprintf("/raw/%s", txID)
	data, err := g.fetch(path)
	if err != nil {
		// 回退到裸端点（某些网关可能不支持 /raw）
		path = fmt.Sprintf("/%s", txID)
		return g.fetch(path)
	}
	return data, nil
}

// FetchRaw 从 /raw 端点获取原始数据（不经过网关的内容类型协商）
func (g *Gateway) FetchRaw(txID string) ([]byte, error) {
	path := fmt.Sprintf("/raw/%s", txID)
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

// FetchRange 获取指定交易的指定字节范围数据
// txID: 交易 ID
// offset: 起始偏移量（字节，从 0 开始）
// length: 读取长度（字节）
func (g *Gateway) FetchRange(txID string, offset, length int64) ([]byte, error) {
	path := fmt.Sprintf("/%s", txID)
	rangeHeader := fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	return g.fetchWithHeaders(path, map[string]string{
		"Range": rangeHeader,
	})
}

// FetchToWriter 流式下载交易数据到 io.Writer，返回写入的字节数
// 与 FetchTransaction 不同，此方法不将全部数据加载到内存，
// 而是用 32KB 缓冲区流式拷贝到 writer（通常是 *os.File）。
// 适用于大文件下载场景。
func (g *Gateway) FetchToWriter(txID string, w io.Writer) (int64, error) {
	path := fmt.Sprintf("/%s", txID)
	return g.fetchToWriter(w, path, nil)
}

// FetchToFile 流式下载交易数据到本地文件
// 返回写入的字节数。若文件已存在，会被覆盖。
func (g *Gateway) FetchToFile(txID string, filePath string) (int64, error) {
	f, err := os.Create(filePath)
	if err != nil {
		return 0, fmt.Errorf("创建文件 %s 失败: %w", filePath, err)
	}
	defer f.Close()

	return g.FetchToWriter(txID, f)
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
				body, err := io.ReadAll(resp.Body)
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

// fetchToWriter 流式 GET 请求，将响应体写入 io.Writer
// 使用 32KB 缓冲区，内存占用可控。返回实际写入的字节数。
func (g *Gateway) fetchToWriter(w io.Writer, path string, headers map[string]string) (int64, error) {
	var lastErr error
	buf := make([]byte, 32*1024) // 32KB 缓冲区

	for attempt := 0; attempt < g.config.MaxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(g.config.RetryDelay)
		}

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
				written, err := io.CopyBuffer(w, resp.Body, buf)
				resp.Body.Close()
				if err != nil {
					lastErr = fmt.Errorf("流式读取响应失败: %w", err)
					g.mu.Lock()
					g.stats.TotalFailures++
					g.mu.Unlock()
					continue
				}

				g.mu.Lock()
				g.stats.TotalSuccess++
				g.stats.BytesDownloaded += uint64(written)
				g.mu.Unlock()

				return written, nil
			}

			resp.Body.Close()

			if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusGone {
				lastErr = fmt.Errorf("网关 %s 返回 %d (交易不存在)", baseURL, resp.StatusCode)
				continue
			}

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

	return 0, fmt.Errorf("所有网关请求失败（%d 次尝试）: %w", g.config.MaxRetries, lastErr)
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
