// Package bridge 提供 IPFAR 桥接核心逻辑
//
// 本文件实现 BlockFetcher — 按需从 Arweave 拉取 IPFS 块。
// 用于 Bitswap 按需架构：
//
//	Bitswap 收到 WantList → BlockFetcher.FetchBlock(cid)
//	  1. 查本地 LRU 缓存 → 命中直接返回
//	  2. 查元数据索引 (index.Store) → 找对应的 Arweave 交易
//	  3. 从 Arweave 按需下载（HTTP Range 请求）
//	  4. 写入缓存 → 返回数据
package bridge

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/lwdjd/IPFAR/internal/cache"
	"github.com/lwdjd/IPFAR/internal/download"
	"github.com/lwdjd/IPFAR/internal/index"
	"github.com/lwdjd/IPFAR/internal/log"
)

// BlockFetcherConfig 按需拉取器配置
type BlockFetcherConfig struct {
	// IndexStore CID → Arweave 位置索引
	IndexStore *index.Store

	// Cache 本地 LRU 块缓存（nil 则自动创建）
	Cache *cache.Cache

	// CacheConfig 缓存配置（当 Cache 为 nil 时使用）
	CacheConfig cache.CacheConfig

	// Gateway 网关客户端（nil 则使用默认）
	Gateway *download.Gateway

	// MaxConcurrentDownloads 最大并发下载数（0 使用默认值 4）
	MaxConcurrentDownloads int

	// Timeout 单次下载超时（0 使用默认值 30s）
	Timeout time.Duration
}

// DefaultBlockFetcherConfig 返回默认配置
func DefaultBlockFetcherConfig() BlockFetcherConfig {
	return BlockFetcherConfig{
		MaxConcurrentDownloads: 4,
		Timeout:                30 * time.Second,
		CacheConfig:            cache.DefaultCacheConfig(),
	}
}

// BlockFetcher 按需拉取 IPFS 块
//
// 实现 bitswap.BlockFetcher 接口，在 Bitswap 收到 WantList 时
// 按需从 Arweave 拉取缺失的块。
type BlockFetcher struct {
	config BlockFetcherConfig

	index   *index.Store
	cache   *cache.Cache
	gateway *download.Gateway

	// 并发控制：带缓冲 channel 作为信号量
	downloadSem chan struct{}

	// 正在进行的下载（去重合并）
	inFlight   map[string]*inFlightReq
	inFlightMu sync.Mutex

	// 统计
	mu       sync.RWMutex
	stats    BlockFetcherStats
}

// inFlightReq 正在进行的请求（用于合并重复请求）
type inFlightReq struct {
	done chan struct{}
	data []byte
	err  error
}

// BlockFetcherStats 统计信息
type BlockFetcherStats struct {
	CacheHits   uint64 `json:"cache_hits"`
	CacheMisses uint64 `json:"cache_misses"`
	IndexHits   uint64 `json:"index_hits"`
	IndexMisses uint64 `json:"index_misses"`
	Downloads   uint64 `json:"downloads"`
	TotalBytes  uint64 `json:"total_bytes"`
}

// NewBlockFetcher 创建按需拉取器
func NewBlockFetcher(cfg BlockFetcherConfig) (*BlockFetcher, error) {
	if cfg.IndexStore == nil {
		return nil, fmt.Errorf("BlockFetcher: IndexStore must not be nil")
	}

	gateway := cfg.Gateway
	if gateway == nil {
		gateway = download.NewGateway(download.DefaultGatewayConfig())
	}

	cacheInst := cfg.Cache
	if cacheInst == nil {
		var err error
		cacheInst, err = cache.New(cfg.CacheConfig)
		if err != nil {
			return nil, fmt.Errorf("BlockFetcher: create cache: %w", err)
		}
	}

	maxConcurrent := cfg.MaxConcurrentDownloads
	if maxConcurrent <= 0 {
		maxConcurrent = 4
	}

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	_ = timeout // used in context deadlines

	bf := &BlockFetcher{
		config:      cfg,
		index:       cfg.IndexStore,
		cache:       cacheInst,
		gateway:     gateway,
		downloadSem: make(chan struct{}, maxConcurrent),
		inFlight:    make(map[string]*inFlightReq),
	}

	log.Info("BlockFetcher：初始化完成 max_concurrent=%d",
		maxConcurrent)
	return bf, nil
}

// FetchBlock 按需获取一个 IPFS 块
//
// 实现 bitswap.BlockFetcher 接口。
// 查找链: 缓存 → 索引 → Arweave 下载 → 缓存 → 返回
func (bf *BlockFetcher) FetchBlock(ctx context.Context, cidStr string) ([]byte, error) {
	if cidStr == "" {
		return nil, fmt.Errorf("BlockFetcher: CID must not be empty")
	}

	// Step 1: 查本地 LRU 缓存
	if bf.cache != nil {
		if data, found, _ := bf.cache.Get(cidStr); found {
			bf.mu.Lock()
			bf.stats.CacheHits++
			bf.mu.Unlock()
			log.Debug("BlockFetcher：缓存命中 %s (%d bytes)", cidStr, len(data))
			return data, nil
		}
	}

	bf.mu.Lock()
	bf.stats.CacheMisses++
	bf.mu.Unlock()

	// 合并重复请求（去重）
	bf.inFlightMu.Lock()
	if req, ok := bf.inFlight[cidStr]; ok {
		bf.inFlightMu.Unlock()
		log.Debug("BlockFetcher：等待正在进行的下载 %s", cidStr)
		select {
		case <-req.done:
			return req.data, req.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// 创建新的 in-flight 请求
	req := &inFlightReq{done: make(chan struct{})}
	bf.inFlight[cidStr] = req
	bf.inFlightMu.Unlock()

	defer func() {
		bf.inFlightMu.Lock()
		delete(bf.inFlight, cidStr)
		bf.inFlightMu.Unlock()
		close(req.done)
	}()

	// Step 2: 查元数据索引
	entry, err := bf.index.Get(cidStr)
	if err != nil {
		req.data = nil
		req.err = err
		return nil, err
	}
	if entry == nil {
		bf.mu.Lock()
		bf.stats.IndexMisses++
		bf.mu.Unlock()
		req.data = nil
		req.err = fmt.Errorf("BlockFetcher: CID %s not indexed", cidStr)
		return nil, req.err
	}

	bf.mu.Lock()
	bf.stats.IndexHits++
	bf.mu.Unlock()

	// Step 3: 从 Arweave 下载
	bf.downloadSem <- struct{}{} // 获取并发槽位
	defer func() { <-bf.downloadSem }()

	data, err := bf.downloadBlock(ctx, entry, cidStr)
	if err != nil {
		req.data = nil
		req.err = err
		return nil, err
	}

	// Step 4: 写入本地缓存
	if bf.cache != nil && data != nil {
		if putErr := bf.cache.Put(cidStr, data); putErr != nil {
			log.Warn("BlockFetcher：缓存写入失败 %s: %v", cidStr, putErr)
		}
	}

	bf.mu.Lock()
	bf.stats.Downloads++
	bf.stats.TotalBytes += uint64(len(data))
	bf.mu.Unlock()

	log.Debug("BlockFetcher：下载完成 %s (%d bytes)", cidStr, len(data))

	req.data = data
	req.err = nil
	return data, nil
}

// downloadBlock 从 Arweave 下载指定 CID 对应的块数据
func (bf *BlockFetcher) downloadBlock(ctx context.Context, entry *index.Entry, cidStr string) ([]byte, error) {
	// 确定下载策略
	dataTXID := entry.DataTXID
	if dataTXID == "" {
		return nil, fmt.Errorf("BlockFetcher: no data_txid for CID %s", cidStr)
	}

	if entry.Height == -1 && entry.BundleTXID != "" && entry.BundleTXID != "none" {
		// 跨 Bundle 模式
		return bf.downloadFromBundle(ctx, entry, cidStr)
	} else if entry.Height == -1 && entry.BundleTXID == "none" {
		// 同 Bundle 模式：需要预先缓存了 Bundle 数据
		// 由于同 Bundle 模式下数据在元数据所在的交易中，
		// 这里需要特殊处理：直接用 dataTXID 下载
		return bf.downloadAsRange(ctx, dataTXID, cidStr, entry)
	}

	// 普通模式：直接 Range 下载
	return bf.downloadAsRange(ctx, dataTXID, cidStr, entry)
}

// downloadAsRange 用 HTTP Range 从 Arweave 下载块数据
//
// 策略：
//  1. 如果有 RawIndex（CAR v2 Index），解析 Index 找到该 CID 的精确偏移
//  2. 如果没有 Index，先下载 CAR 文件头部解析索引
//  3. 使用 Range 请求下载该块数据
//
// 当前简化实现：直接用 Range 下载整个交易数据中该 CID 对应的部分。
// 如果 metadata 没有记录精确偏移，则下载完整数据（fallback）。
func (bf *BlockFetcher) downloadAsRange(ctx context.Context, txID string, cidStr string, entry *index.Entry) ([]byte, error) {
	// 如果有 RawIndex，尝试用 CAR v2 Index 定位块偏移
	if len(entry.RawIndex) > 0 {
		return bf.downloadBlockWithRawIndex(ctx, txID, cidStr, entry)
	}

	// Fallback：没有索引信息，下载完整数据
	// 这对于小块数据是可行的
	log.Debug("BlockFetcher：无 CAR Index，下载完整交易数据 %s (size=%d)", txID, entry.DataSize)

	// 使用网关直接下载
	data, err := bf.gateway.FetchRaw(txID)
	if err != nil {
		return nil, fmt.Errorf("BlockFetcher: download tx %s: %w", txID, err)
	}

	// 注意：这里是完整 CAR 数据。在实际优化中应该解析 CAR 提取指定块。
	// 当前 fallback 返回完整数据，上层 Bitswap 会验证 CID 匹配。
	return data, nil
}

// downloadBlockWithRawIndex 使用 CAR v2 原始索引（raw_index）定位块
func (bf *BlockFetcher) downloadBlockWithRawIndex(ctx context.Context, txID string, cidStr string, entry *index.Entry) ([]byte, error) {
	// raw_index 是 CAR v2 Index 的序列化字节
	// 解析 Index 找到对应 CID 的块偏移
	// 注意：这里需要解析 CAR v2 Index 格式
	//
	// CAR v2 Index 格式：
	//   - 每个条目: CID(36 bytes?) + offset(8 bytes LE)
	// 简化实现：使用 SDK 的 CAR 解析（如有）或从完整数据中提取
	//
	// TODO: 实现精确的 CAR v2 Index 解析
	// 目前 fallback 到下载完整数据
	return bf.downloadAsRange(ctx, txID, cidStr, entry)
}

// downloadFromBundle 从跨 Bundle 中下载块数据
func (bf *BlockFetcher) downloadFromBundle(ctx context.Context, entry *index.Entry, cidStr string) ([]byte, error) {
	_, rawData, err := bf.gateway.FetchBundleItemByID(entry.BundleTXID, entry.DataTXID)
	if err != nil {
		return nil, fmt.Errorf("BlockFetcher: fetch bundle item %s from bundle %s for CID %s: %w",
			entry.DataTXID, entry.BundleTXID, cidStr, err)
	}
	return rawData, nil
}

// Stats 返回统计信息
func (bf *BlockFetcher) Stats() BlockFetcherStats {
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	return bf.stats
}

// Cache 返回内部缓存实例
func (bf *BlockFetcher) Cache() *cache.Cache {
	return bf.cache
}

// IndexStore 返回内部索引存储
func (bf *BlockFetcher) IndexStore() *index.Store {
	return bf.index
}
