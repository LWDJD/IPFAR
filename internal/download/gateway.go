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

	"github.com/LWDJD/ipfar-sdk/arweave"
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
// 嵌入 SDK 的 MultiGatewayClient 实现多网关自动故障转移，
// 同时保留桥独有的 SOCKS5 代理、统计等功能。
type Gateway struct {
	*arweave.MultiGatewayClient
	config GatewayConfig
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

	httpClient := &http.Client{
		Timeout:   config.Timeout,
		Transport: transport,
	}

	// 创建 SDK MultiGatewayClient 并注入代理 HTTP 客户端
	mgc := arweave.NewMultiGatewayClient(config.URLs...)
	mgc.SetHTTPClient(httpClient)
	if config.UserAgent != "" {
		mgc.SetUserAgent(config.UserAgent)
	}

	return &Gateway{
		MultiGatewayClient: mgc,
		config:             config,
	}
}

// =============================================================================
// 委托给 SDK 的方法（签名对齐后直接调用）
// =============================================================================

// FetchTransaction 获取 Arweave 交易数据（原始字节）
// txID: Arweave 交易 ID（Base64URL）
func (g *Gateway) FetchTransaction(txID string) ([]byte, error) {
	return g.fetchWithRetry(txID, func() ([]byte, error) {
		return g.DownloadTransactionData(context.Background(), txID)
	})
}

// FetchTransactionData 获取交易关联的数据
// txID: Arweave 交易 ID
func (g *Gateway) FetchTransactionData(txID string) ([]byte, error) {
	return g.fetchWithRetry(txID, func() ([]byte, error) {
		return g.DownloadTransactionData(context.Background(), txID)
	})
}

// FetchRaw 获取原始数据（委托给 SDK 的 DownloadTransactionData）
func (g *Gateway) FetchRaw(txID string) ([]byte, error) {
	return g.fetchWithRetry(txID, func() ([]byte, error) {
		return g.DownloadTransactionData(context.Background(), txID)
	})
}

// FetchRange 获取指定交易的指定字节范围数据
// txID: 交易 ID
// offset: 起始偏移量（字节，从 0 开始）
// length: 读取长度（字节）
func (g *Gateway) FetchRange(txID string, offset, length int64) ([]byte, error) {
	return g.fetchRangeWithRetry(txID, offset, length, func() ([]byte, error) {
		return g.DownloadTransactionRange(context.Background(), txID, offset, length)
	})
}

// fetchWithRetry 带重试的数据获取包装器
// 处理 429 限流重试和错误消息格式化
func (g *Gateway) fetchWithRetry(txID string, doFetch func() ([]byte, error)) ([]byte, error) {
	var lastErr error
	for attempt := 0; attempt < g.config.MaxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(g.config.RetryDelay)
		}
		data, err := doFetch()
		if err == nil {
			g.updateStats(true, len(data))
			return data, nil
		}
		lastErr = err
		// 检查是否为 429 限流错误，若是则继续重试
		if isRateLimitError(err) {
			log.Debug("下载：网关限流 (429)，等待重试 (attempt %d/%d)", attempt+1, g.config.MaxRetries)
			continue
		}
		// 非 429 错误：不再重试，直接返回包装后的错误
		break
	}
	g.updateStats(false, 0)
	return nil, fmt.Errorf("所有网关请求失败（%d 次尝试）: %w", g.config.MaxRetries, lastErr)
}

// fetchRangeWithRetry 带重试的范围数据获取包装器
func (g *Gateway) fetchRangeWithRetry(txID string, offset, length int64, doFetch func() ([]byte, error)) ([]byte, error) {
	var lastErr error
	for attempt := 0; attempt < g.config.MaxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(g.config.RetryDelay)
		}
		data, err := doFetch()
		if err == nil {
			g.updateStats(true, len(data))
			return data, nil
		}
		lastErr = err
		if isRateLimitError(err) {
			log.Debug("下载：网关限流 (429)，等待重试 (attempt %d/%d)", attempt+1, g.config.MaxRetries)
			continue
		}
		break
	}
	g.updateStats(false, 0)
	return nil, fmt.Errorf("所有网关请求失败（%d 次尝试）: %w", g.config.MaxRetries, lastErr)
}

// isRateLimitError 检查错误是否为 HTTP 429 限流错误
func isRateLimitError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "429")
}

// FetchToWriter 流式下载交易数据到 io.Writer
// 使用 32KB 缓冲区流式拷贝到 writer（通常是 *os.File）。
// 适用于大文件下载场景。
func (g *Gateway) FetchToWriter(txID string, w io.Writer) (int64, error) {
	written, err := g.DownloadTransactionToWriter(context.Background(), txID, w)
	if err == nil {
		g.mu.Lock()
		g.stats.TotalSuccess++
		g.stats.BytesDownloaded += uint64(written)
		g.mu.Unlock()
	} else {
		g.mu.Lock()
		g.stats.TotalFailures++
		g.mu.Unlock()
	}
	g.mu.Lock()
	g.stats.TotalRequests++
	g.mu.Unlock()
	return written, err
}

// FetchBundleItemByID 通过 Bundle TXID 和 Item ID 获取 Bundle 中的指定数据项
//
// 委托给 SDK 的 FetchBundleItemByID，SDK 返回已解码的纯数据。
// 为保持向后兼容，方法签名返回 (itemData, rawData, error)，
// 其中 itemData 始终为 nil（SDK 不返回原始 ANS-104 二进制），
// rawData 为提取后的纯数据。
//
// 参数:
//   - bundleTXID: Bundle 交易的 Arweave TX ID
//   - itemID: Bundle Item 的 ID（Base64URL 编码，即 data_txid）
//
// 返回:
//   - itemData: 始终为 nil（SDK 已提取纯数据）
//   - rawData: Item 的纯数据部分（不含 ANS-104 头部）
func (g *Gateway) FetchBundleItemByID(bundleTXID, itemID string) (itemData []byte, rawData []byte, err error) {
	data, err := g.MultiGatewayClient.FetchBundleItemByID(context.Background(), bundleTXID, itemID)
	if err == nil {
		g.mu.Lock()
		g.stats.TotalRequests++
		g.stats.TotalSuccess++
		g.stats.BytesDownloaded += uint64(len(data))
		g.mu.Unlock()
	} else {
		g.mu.Lock()
		g.stats.TotalRequests++
		g.stats.TotalFailures++
		g.mu.Unlock()
	}
	return nil, data, err
}

// =============================================================================
// 桥独有功能（保留）
// =============================================================================

// FetchChunk 获取指定交易的指定偏移量数据块
// txID: 交易 ID
// offset: 数据偏移量（字节）
func (g *Gateway) FetchChunk(txID string, offset int64) ([]byte, error) {
	var data []byte
	ctx := context.Background()

	err := g.Do(ctx, func(gw *arweave.GatewayClient) error {
		url := gw.GatewayURL + "/" + txID
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return fmt.Errorf("创建请求失败: %w", err)
		}
		req.Header.Set("User-Agent", g.config.UserAgent)
		req.Header.Set("Accept", "*/*")
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))

		g.mu.Lock()
		g.stats.TotalRequests++
		g.mu.Unlock()

		resp, err := gw.HTTPClient().Do(req)
		if err != nil {
			g.mu.Lock()
			g.stats.TotalFailures++
			g.mu.Unlock()
			return fmt.Errorf("网关 %s 请求失败: %w", gw.GatewayURL, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusPartialContent {
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				g.mu.Lock()
				g.stats.TotalFailures++
				g.mu.Unlock()
				return fmt.Errorf("读取响应失败: %w", err)
			}
			g.mu.Lock()
			g.stats.TotalSuccess++
			g.stats.BytesDownloaded += uint64(len(body))
			g.mu.Unlock()
			data = body
			return nil
		}

		if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusGone {
			return fmt.Errorf("网关 %s 返回 %d (交易不存在)", gw.GatewayURL, resp.StatusCode)
		}
		return fmt.Errorf("网关 %s 返回 HTTP %d", gw.GatewayURL, resp.StatusCode)
	})

	return data, err
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
	var header http.Header
	ctx := context.Background()

	err := g.Do(ctx, func(gw *arweave.GatewayClient) error {
		url := gw.GatewayURL + "/" + txID
		req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
		if err != nil {
			return err
		}
		req.Header.Set("User-Agent", g.config.UserAgent)

		g.mu.Lock()
		g.stats.TotalRequests++
		g.mu.Unlock()

		resp, err := gw.HTTPClient().Do(req)
		if err != nil {
			g.mu.Lock()
			g.stats.TotalFailures++
			g.mu.Unlock()
			return err
		}
		resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			g.mu.Lock()
			g.stats.TotalSuccess++
			g.mu.Unlock()
			header = resp.Header
			return nil
		}

		return fmt.Errorf("HEAD %s: HTTP %d", gw.GatewayURL, resp.StatusCode)
	})

	return header, err
}

// FetchBlockByHeight 获取指定高度的区块信息
func (g *Gateway) FetchBlockByHeight(height uint64) ([]byte, error) {
	var data []byte
	ctx := context.Background()

	err := g.Do(ctx, func(gw *arweave.GatewayClient) error {
		url := gw.GatewayURL + "/block/height/" + fmt.Sprintf("%d", height)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return fmt.Errorf("创建请求失败: %w", err)
		}
		req.Header.Set("User-Agent", g.config.UserAgent)
		req.Header.Set("Accept", "*/*")

		g.mu.Lock()
		g.stats.TotalRequests++
		g.mu.Unlock()

		resp, err := gw.HTTPClient().Do(req)
		if err != nil {
			g.mu.Lock()
			g.stats.TotalFailures++
			g.mu.Unlock()
			return fmt.Errorf("网关 %s 请求失败: %w", gw.GatewayURL, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				g.mu.Lock()
				g.stats.TotalFailures++
				g.mu.Unlock()
				return fmt.Errorf("读取响应失败: %w", err)
			}
			g.mu.Lock()
			g.stats.TotalSuccess++
			g.stats.BytesDownloaded += uint64(len(body))
			g.mu.Unlock()
			data = body
			return nil
		}

		if resp.StatusCode == http.StatusNotFound {
			return fmt.Errorf("网关 %s 返回 404 (区块不存在)", gw.GatewayURL)
		}
		return fmt.Errorf("网关 %s 返回 HTTP %d", gw.GatewayURL, resp.StatusCode)
	})

	return data, err
}

// Stats 返回网关统计信息
func (g *Gateway) Stats() GatewayStats {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.stats
}

// updateStats 更新统计信息的辅助方法
func (g *Gateway) updateStats(success bool, bytesDownloaded int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.stats.TotalRequests++
	if success {
		g.stats.TotalSuccess++
		g.stats.BytesDownloaded += uint64(bytesDownloaded)
	} else {
		g.stats.TotalFailures++
	}
}
