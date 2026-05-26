// Package bridge 提供 IPFAR 桥接核心逻辑
//
// 本文件实现 BlockFetcher — 按需从 Arweave 拉取 IPFS 块。
// 用于 Bitswap 按需架构：
//
//	Bitswap 收到 WantList → BlockFetcher.FetchBlock(cid)
//	  1. 查本地 LRU 缓存 → 命中直接返回
//	  2. 查第一层索引 (CID → metaTxID 列表)
//	  3. 查第二层索引 (metaTxID → 参数: dataTxId, bundleTxId, blockHeight...)
//	  4. 从 Arweave 按需下载
//	  5. 写入缓存 → 返回数据
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

// StrictVerifier 严格级别验证器函数类型
// 对指定 metaTxID 执行 strict 级别全步骤验证并缓存结果
type StrictVerifier func(ctx context.Context, metaTxID string) error

// BlockFetcherConfig 按需拉取器配置
type BlockFetcherConfig struct {
	// IndexStore CID → Arweave 位置索引（两层索引）
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

	// StrictVerifier 严格级别验证器（用于 Bitswap 请求时确保数据有效性）
	StrictVerifier StrictVerifier
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

	// strictVerifier 严格级别验证器（nil 表示跳过 strict 验证）
	strictVerifier StrictVerifier

	// 并发控制：带缓冲 channel 作为信号量
	downloadSem chan struct{}

	// 正在进行的下载（去重合并）
	inFlight   map[string]*inFlightReq
	inFlightMu sync.Mutex

	// 统计
	mu    sync.RWMutex
	stats BlockFetcherStats
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
		config:         cfg,
		index:          cfg.IndexStore,
		cache:          cacheInst,
		gateway:        gateway,
		strictVerifier: cfg.StrictVerifier,
		downloadSem:    make(chan struct{}, maxConcurrent),
		inFlight:       make(map[string]*inFlightReq),
	}

	log.Info("BlockFetcher：初始化完成 max_concurrent=%d", maxConcurrent)
	return bf, nil
}

// FetchBlock 按需获取一个 IPFS 块
//
// 查找链: 缓存 → 第一层索引(CID→metaTxIDs) → 第二层索引(metaTxID→params) → Arweave 下载 → 缓存 → 返回
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

	// Step 2: 查第一层索引：CID → metaTxID 列表
	metaIDs, err := bf.index.GetCIDMetaIDs(cidStr)
	if err != nil {
		req.data = nil
		req.err = err
		return nil, err
	}
	if len(metaIDs) == 0 {
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

	// Step 3: 查第二层索引：取第一个 metaTxID 的参数
	// 遍历所有 metaTxID，找到第一个可下载的
	var data []byte
	var lastErr error
	for _, metaTxID := range metaIDs {
		params, err := bf.index.GetMeta(metaTxID)
		if err != nil {
			lastErr = err
			log.Debug("BlockFetcher：获取元数据失败 metaTxID=%s: %v", metaTxID, err)
			continue
		}
		if params == nil {
			continue
		}

		// Step 3.5: 严格级别验证（Bitswap 请求时升到最高级别）
		if err := bf.ensureStrictVerified(ctx, metaTxID); err != nil {
			lastErr = fmt.Errorf("BlockFetcher: strict verification failed for %s: %w", metaTxID, err)
			log.Warn("BlockFetcher：strict 验证失败 metaTxID=%s: %v，尝试下一个来源", metaTxID, err)
			continue
		}

		dataTxID := params["dataTxId"]
		bundleTxID := params["bundleTxId"]
		blockHeightStr := params["blockHeight"]

		if dataTxID == "" {
			continue
		}

		// Step 4: 从 Arweave 下载
		bf.downloadSem <- struct{}{} // 获取并发槽位

		var blockHeight int64
		if blockHeightStr != "" {
			fmt.Sscanf(blockHeightStr, "%d", &blockHeight)
		}

		data, lastErr = bf.downloadBlock(ctx, dataTxID, bundleTxID, blockHeight, cidStr)
		<-bf.downloadSem // 释放槽位

		if lastErr == nil && data != nil {
			break
		}
		log.Debug("BlockFetcher：下载失败 metaTxID=%s dataTxID=%s: %v", metaTxID, dataTxID, lastErr)
	}

	if data == nil {
		if lastErr != nil {
			req.data = nil
			req.err = lastErr
			return nil, lastErr
		}
		req.data = nil
		req.err = fmt.Errorf("BlockFetcher: CID %s not downloadable from any known source", cidStr)
		return nil, req.err
	}

	// Step 5: 写入本地缓存
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
func (bf *BlockFetcher) downloadBlock(ctx context.Context, dataTXID, bundleTXID string, blockHeight int64, cidStr string) ([]byte, error) {
	if dataTXID == "" {
		return nil, fmt.Errorf("BlockFetcher: no data_txid for CID %s", cidStr)
	}

	if blockHeight == -1 && bundleTXID != "" && bundleTXID != "none" {
		// 跨 Bundle 模式
		_, rawData, err := bf.gateway.FetchBundleItemByID(bundleTXID, dataTXID)
		if err != nil {
			return nil, fmt.Errorf("BlockFetcher: fetch bundle item %s from bundle %s: %w",
				dataTXID, bundleTXID, err)
		}
		return rawData, nil
	} else if blockHeight == -1 && bundleTXID == "none" {
		// 同 Bundle 模式：直接用 dataTXID 下载
		return bf.downloadAsRaw(ctx, dataTXID)
	}

	// 普通模式：直接下载
	return bf.downloadAsRaw(ctx, dataTXID)
}

// downloadAsRaw 直接下载交易数据
func (bf *BlockFetcher) downloadAsRaw(ctx context.Context, txID string) ([]byte, error) {
	log.Debug("BlockFetcher：直接下载交易 %s", txID)
	data, err := bf.gateway.FetchRaw(txID)
	if err != nil {
		return nil, fmt.Errorf("BlockFetcher: download tx %s: %w", txID, err)
	}
	return data, nil
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

// SetStrictVerifier 设置严格级别验证器
func (bf *BlockFetcher) SetStrictVerifier(v StrictVerifier) {
	bf.strictVerifier = v
}

// ensureStrictVerified 确保指定 metaTxID 已通过 strict 级别全步骤验证
//
// 在从 Arweave 下载块数据之前调用，确保数据源可信。
// 检查顺序：pow → index → ref_chain → integrity
// 如果 strict 级别已全部缓存，直接返回 nil（跳过验证）。
// 验证失败则返回错误（调用方应跳过该 metaTxID，尝试下一个来源）。
func (bf *BlockFetcher) ensureStrictVerified(ctx context.Context, metaTxID string) error {
	steps := []string{index.StepPoW, index.StepIndex, index.StepRefChain, index.StepIntegrity}

	// 检查 strict 级别所有步骤是否已验证
	allCached := true
	for _, step := range steps {
		ok, err := bf.index.IsStepVerifiedAtLevel(metaTxID, step, index.LevelStrict)
		if err != nil {
			log.Warn("BlockFetcher：检查 strict 验证缓存失败 step=%s metaTxID=%s: %v", step, metaTxID, err)
			allCached = false
			break
		}
		if !ok {
			allCached = false
			break
		}
	}

	if allCached {
		log.Debug("BlockFetcher：strict 级别全部步骤已缓存 metaTxID=%s", metaTxID)
		return nil
	}

	// 需要执行 strict 验证
	if bf.strictVerifier == nil {
		return fmt.Errorf("BlockFetcher: strict verification required for %s but no verifier configured", metaTxID)
	}

	log.Info("BlockFetcher：开始 strict 级别验证 metaTxID=%s", metaTxID)
	if err := bf.strictVerifier(ctx, metaTxID); err != nil {
		return fmt.Errorf("BlockFetcher: strict verification failed for %s: %w", metaTxID, err)
	}

	// 验证后再次检查所有步骤是否已标记
	for _, step := range steps {
		ok, err := bf.index.IsStepVerifiedAtLevel(metaTxID, step, index.LevelStrict)
		if err != nil {
			return fmt.Errorf("BlockFetcher: post-verification cache check failed step=%s: %w", step, err)
		}
		if !ok {
			return fmt.Errorf("BlockFetcher: strict verification incomplete: step %s not marked for %s", step, metaTxID)
		}
	}

	log.Info("BlockFetcher：strict 级别验证通过 metaTxID=%s", metaTxID)
	return nil
}
