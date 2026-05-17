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

// FetchBundleItemByID 通过 Bundle TXID 和 Item ID 获取 Bundle 中的指定数据项
//
// 流程：
//  1. 解析 Bundle 头部（HTTP Range 请求前 32 + N*64 字节）
//  2. 在索引中查找匹配 itemID 的 Item
//  3. 用 HTTP Range 请求下载该 Item 的二进制数据
//
// 参数:
//   - bundleTXID: Bundle 交易的 Arweave TX ID
//   - itemID: Bundle Item 的 ID（Base64URL 编码，即 data_txid）
//
// 返回:
//   - itemData: Item 的原始二进制数据（含签名、owner、tags 等头部）
//   - rawData: Item 的纯数据部分（不含 ANS-104 头部）
func (g *Gateway) FetchBundleItemByID(bundleTXID, itemID string) (itemData []byte, rawData []byte, err error) {
	// Step 1: 解析 Bundle 头部获取 item 数量
	// 头部前 32 字节 = item 数量（小端序 int64）
	header32 := make([]byte, 32)
	path := fmt.Sprintf("/raw/%s", bundleTXID)
	data, err := g.fetchWithHeaders(path, map[string]string{
		"Range": "bytes=0-31",
	})
	if err != nil {
		return nil, nil, fmt.Errorf("获取 Bundle 头部失败: %w", err)
	}
	copy(header32, data)

	itemsNum := byteArrayToLongLE(header32)
	if itemsNum <= 0 || itemsNum > 100000 {
		return nil, nil, fmt.Errorf("Bundle item 数量异常: %d", itemsNum)
	}

	// Step 2: 下载完整头部 (32 + itemsNum*64 字节)
	headerSize := 32 + itemsNum*64
	fullHeader, err := g.fetchWithHeaders(path, map[string]string{
		"Range": fmt.Sprintf("bytes=0-%d", headerSize-1),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("获取 Bundle 完整头部失败: %w", err)
	}

	// Step 3: 在索引中查找匹配 itemID 的 item
	// 头部格式: [32 bytes item_count][N * (32 bytes length + 32 bytes id)]
	var targetOffset int64 = int64(headerSize) // 数据区起始偏移
	var targetLength int64 = -1
	found := false

	for i := int64(0); i < itemsNum; i++ {
		metaOffset := 32 + int(i)*64
		if metaOffset+64 > len(fullHeader) {
			break
		}
		itemLen := byteArrayToLongLE(fullHeader[metaOffset : metaOffset+32])
		itemIDBytes := fullHeader[metaOffset+32 : metaOffset+64]
		currentItemID := base64URLEncode(itemIDBytes)

		if currentItemID == itemID {
			targetLength = itemLen
			found = true
			break
		}
		targetOffset += itemLen
	}

	if !found {
		return nil, nil, fmt.Errorf("在 Bundle %s 中未找到 Item %s", bundleTXID, itemID)
	}

	if targetLength <= 0 || targetLength > 256*1024*1024 { // 最大 256MB
		return nil, nil, fmt.Errorf("Bundle Item 长度异常: %d", targetLength)
	}

	// Step 4: 下载 Item 数据
	itemData, err = g.fetchWithHeaders(path, map[string]string{
		"Range": fmt.Sprintf("bytes=%d-%d", targetOffset, targetOffset+targetLength-1),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("下载 Bundle Item 失败: %w", err)
	}

	// Step 5: 提取纯数据部分（跳过 ANS-104 头部）
	// ANS-104 Item 格式: [2B sigType][sig][owner][1B targetPresent][?target][1B anchorPresent][?anchor][8B tagCount][8B tagsLength][tags][data]
	rawData, err = extractBundleItemData(itemData)
	if err != nil {
		return itemData, nil, fmt.Errorf("提取 Bundle Item 数据失败: %w", err)
	}

	return itemData, rawData, nil
}

// byteArrayToLongLE 小端序字节数组转 int64
func byteArrayToLongLE(b []byte) int64 {
	var result int64
	for i := 0; i < len(b) && i < 8; i++ {
		result |= int64(b[i]) << (8 * i)
	}
	return result
}

// base64URLEncode Base64URL 编码（无填充）
func base64URLEncode(data []byte) string {
	// 使用标准库的 RawURLEncoding
	enc := make([]byte, ((len(data)+2)/3)*4)
	n := base64RawURLEncode(enc, data)
	return string(enc[:n])
}

// base64RawURLEncode 简单的 Base64URL 编码（无填充）
func base64RawURLEncode(dst, src []byte) int {
	alphabet := "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	n := 0
	for len(src) > 0 {
		var b0, b1, b2, b3 byte

		if len(src) > 0 {
			b0 = src[0] >> 2
		}
		if len(src) > 1 {
			b1 = ((src[0] & 0x3) << 4) | (src[1] >> 4)
		} else {
			b1 = (src[0] & 0x3) << 4
		}
		if len(src) > 2 {
			b2 = ((src[1] & 0xF) << 2) | (src[2] >> 6)
		} else if len(src) > 1 {
			b2 = (src[1] & 0xF) << 2
		}
		if len(src) > 2 {
			b3 = src[2] & 0x3F
		}

		dst[n] = alphabet[b0]
		n++
		dst[n] = alphabet[b1]
		n++

		if len(src) > 1 {
			dst[n] = alphabet[b2]
			n++
		}
		if len(src) > 2 {
			dst[n] = alphabet[b3]
			n++
		}

		if len(src) > 3 {
			src = src[3:]
		} else {
			break
		}
	}
	return n
}

// extractBundleItemData 从 ANS-104 Bundle Item 二进制中提取纯数据
//
// ANS-104 Item 二进制格式:
//
//	[2B  signatureType]
//	[NB  signature]           // SigConfigMap[signatureType].SigLength
//	[NB  owner]               // SigConfigMap[signatureType].PubLength
//	[1B  targetPresent]
//	[32B target]              // 仅当 targetPresent=1
//	[1B  anchorPresent]
//	[32B anchor]              // 仅当 anchorPresent=1
//	[8B  numOfTags LE]
//	[8B  tagsBytesLength LE]
//	[NB  tagsBytes]           // Avro 编码的 Tags
//	[NB  data]                // ← 我们要提取的部分
func extractBundleItemData(itemBinary []byte) ([]byte, error) {
	if len(itemBinary) < 2 {
		return nil, fmt.Errorf("item 数据太短")
	}

	sigType := int(itemBinary[0]) | int(itemBinary[1])<<8

	// 签名类型配置（与 SDK arweave 包保持一致）
	type sigMeta struct {
		sigLength int
		pubLength int
	}
	sigConfigMap := map[int]sigMeta{
		1: {512, 512}, // Arweave RSA
		2: {64, 32},   // ED25519
		3: {65, 65},   // Ethereum
		4: {64, 32},   // Solana
	}

	cfg, ok := sigConfigMap[sigType]
	if !ok {
		return nil, fmt.Errorf("不支持的签名类型: %d", sigType)
	}

	pos := 2 + cfg.sigLength + cfg.pubLength

	if pos+1 > len(itemBinary) {
		return nil, fmt.Errorf("item 数据不完整: 无法读取 target present 标志")
	}

	targetPresent := itemBinary[pos] == 1
	pos++

	if targetPresent {
		if pos+32 > len(itemBinary) {
			return nil, fmt.Errorf("item 数据不完整: target 数据越界")
		}
		pos += 32
	}

	if pos+1 > len(itemBinary) {
		return nil, fmt.Errorf("item 数据不完整: 无法读取 anchor present 标志")
	}

	anchorPresent := itemBinary[pos] == 1
	pos++

	if anchorPresent {
		if pos+32 > len(itemBinary) {
			return nil, fmt.Errorf("item 数据不完整: anchor 数据越界")
		}
		pos += 32
	}

	// 跳过 numOfTags (8 bytes) 和 tagsBytesLength (8 bytes)
	if pos+16 > len(itemBinary) {
		return nil, fmt.Errorf("item 数据不完整: 无法读取 tag 元数据")
	}

	// tagsBytesLength（小端序 int64）
	tagsLength := int64(0)
	for i := 0; i < 8; i++ {
		tagsLength |= int64(itemBinary[pos+8+i]) << (8 * i)
	}
	pos += 16

	if tagsLength < 0 || pos+int(tagsLength) > len(itemBinary) {
		return nil, fmt.Errorf("item 数据不完整: tags 数据越界 (tagsLength=%d, pos=%d, len=%d)", tagsLength, pos, len(itemBinary))
	}
	pos += int(tagsLength)

	// 剩余部分就是纯数据
	return itemBinary[pos:], nil
}
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
