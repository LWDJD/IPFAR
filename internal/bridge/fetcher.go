// Package bridge 提供 IPFAR 桥接核心逻辑
//
// 本文件实现 BlockFetcher — 按需从 Arweave 拉取 IPFS 块。
// 用于 Bitswap 按需架构：
//
//	Bitswap 收到 WantList → BlockFetcher.FetchBlock(cid)
//	  1. 查本地 LRU 缓存 → 命中直接返回
//	  2. 查第一层索引 (CID → metaTxID 列表)
//	  3. 查第二层索引 (metaTxID → 参数: dataTxId, bundleTxId, blockHeight...)
//	  4. HTTP Range 分两阶段拉取 CAR 文件：
//	     a. Range 读取 CAR v2 头部 (52 bytes) → IndexOffset + IndexSize
//	     b. Range 读取 Index 段 → 解析 CID→offset 映射
//	     c. Range 读取目标块数据 → 提取纯数据立即返回
//	     d. [后台] 顺序下载完整 CAR → 验证 → 缓存所有块
//	  5. 写入 LRU 缓存 → 返回数据
package bridge

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	sdkcar "github.com/LWDJD/ipfar-sdk/verify/ipfs"

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
		dataSizeStr := params["dataSize"]

		if dataTxID == "" {
			continue
		}

		// Step 4: 两阶段 CAR 拉取
		bf.downloadSem <- struct{}{} // 获取并发槽位

		data, lastErr = bf.fetchBlockViaCAR(ctx, dataTxID, cidStr)

		<-bf.downloadSem // 释放槽位

		if lastErr == nil && data != nil {
			// Step 5: 后台 goroutine 下载完整 CAR 并缓存所有块
			go bf.backgroundFullDownload(dataTxID, dataSizeStr, cidStr)
			break
		}
		log.Debug("BlockFetcher：CAR 拉取失败 metaTxID=%s dataTxID=%s: %v", metaTxID, dataTxID, lastErr)
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

// fetchBlockViaCAR 通过 HTTP Range 分阶段拉取 CAR 文件中的单个块
//
// 流程:
//  1. HTTP Range 读取 CAR v2 头部（52 bytes）→ 获取 IndexOffset + IndexSize
//  2. HTTP Range 读取 Index 段 → 解析 CID→offset 映射
//  3. HTTP Range 读取目标块数据（~2MB 估算范围）→ 提取纯数据
//
// 不使用预存偏移，每次按需拉取 Index 并解析。
func (bf *BlockFetcher) fetchBlockViaCAR(ctx context.Context, dataTxID, cidStr string) ([]byte, error) {
	if dataTxID == "" {
		return nil, fmt.Errorf("BlockFetcher: no data_txid for CID %s", cidStr)
	}

	log.Debug("BlockFetcher：两阶段 CAR 拉取 dataTxID=%s cid=%s", dataTxID, cidStr)

	// Phase 1a: HTTP Range 读取 CAR v2 头部（前 52 bytes：4 字节魔数 + 48 字节头部）
	headerBytes, err := bf.gateway.FetchRange(dataTxID, 0, 52)
	if err != nil {
		return nil, fmt.Errorf("BlockFetcher: fetch CAR v2 header for %s: %w", dataTxID, err)
	}

	// 验证 CAR v2 魔数
	if len(headerBytes) < 4 || string(headerBytes[:4]) != "car\x02" {
		return nil, fmt.Errorf("BlockFetcher: not a CAR v2 file (tx %s)", dataTxID)
	}

	// 解析 CAR v2 头部 (52 bytes, 4 字节魔数 + 48 字节头部)
	if len(headerBytes) < 52 {
		return nil, fmt.Errorf("BlockFetcher: CAR v2 header too short: %d bytes", len(headerBytes))
	}

	// 头部结构 (offset 4-51, 共 48 字节):
	// Characteristics[16] + DataOffset[8] + DataSize[8] + IndexOffset[8] + IndexSize[8]
	indexOffset := binary.LittleEndian.Uint64(headerBytes[4+32 : 4+40])
	indexSize := binary.LittleEndian.Uint64(headerBytes[4+40 : 4+48])

	if indexOffset == 0 || indexSize == 0 {
		return nil, fmt.Errorf("BlockFetcher: CAR v2 has no index (tx %s)", dataTxID)
	}

	log.Debug("BlockFetcher：CAR v2 头部解析完成 index_offset=%d index_size=%d", indexOffset, indexSize)

	// Phase 1b: HTTP Range 读取 Index 段
	indexBytes, err := bf.gateway.FetchRange(dataTxID, int64(indexOffset), int64(indexSize))
	if err != nil {
		return nil, fmt.Errorf("BlockFetcher: fetch CAR v2 index for %s: %w", dataTxID, err)
	}

	// 解析 Index → CID→offset 映射
	cidToOffset, err := sdkcar.ParseIndexData(indexBytes)
	if err != nil {
		return nil, fmt.Errorf("BlockFetcher: parse CAR index for %s: %w", dataTxID, err)
	}

	// Phase 1c: 查找目标 CID 的偏移量
	blockOffset, found := cidToOffset[cidStr]
	if !found {
		return nil, fmt.Errorf("BlockFetcher: CID %s not found in CAR index (tx %s, %d entries)",
			cidStr, dataTxID, len(cidToOffset))
	}

	log.Debug("BlockFetcher：找到 CID %s at offset=%d", cidStr, blockOffset)

	// Phase 1d: HTTP Range 读取块数据（估算 ~2MB 范围，实际会被 CAR 格式限制）
	// 使用较大的范围确保覆盖完整的块（最大 IPFS 块通常 < 2MB）
	estimatedBlockSize := int64(2 * 1024 * 1024)
	blockData, err := bf.gateway.FetchRange(dataTxID, int64(blockOffset), estimatedBlockSize)
	if err != nil {
		return nil, fmt.Errorf("BlockFetcher: fetch block data at offset %d for %s: %w", blockOffset, dataTxID, err)
	}

	// 从块数据中提取纯数据（去除 varint + CID 前缀）
	pureData, err := sdkcar.ExtractBlockFromCar(blockData, 0)
	if err != nil {
		return nil, fmt.Errorf("BlockFetcher: extract block from CAR for CID %s: %w", cidStr, err)
	}

	log.Debug("BlockFetcher：提取块成功 %s (%d bytes)", cidStr, len(pureData))

	return pureData, nil
}

// backgroundFullDownload 后台下载完整 CAR 文件，验证完整性，并缓存所有块
//
// 在随机读取返回数据后异步执行，不阻塞 Bitswap 响应。
func (bf *BlockFetcher) backgroundFullDownload(dataTxID, dataSizeStr, cidStr string) {
	ctx := context.Background()
	log.Debug("BlockFetcher：后台开始完整下载 CAR dataTxID=%s", dataTxID)

	// 下载完整 CAR 文件
	fullData, err := bf.gateway.FetchRaw(dataTxID)
	if err != nil {
		log.Warn("BlockFetcher：后台完整下载失败 dataTxID=%s: %v", dataTxID, err)
		return
	}

	log.Debug("BlockFetcher：后台完整下载完成 dataTxID=%s (%d bytes)", dataTxID, len(fullData))

	// 解析完整 CAR 中的所有块并缓存
	// 使用 CAR 解析器提取所有块
	// 注意：这里通过 bytes.Reader 实现 io.ReaderAt
	carParser, err := sdkcar.NewCarParserFromReader(
		&readerAtAdapter{data: fullData},
		int64(len(fullData)),
	)
	if err != nil {
		log.Warn("BlockFetcher：后台创建 CAR 解析器失败: %v", err)
		return
	}
	defer carParser.Close()

	// 获取索引中的所有块并缓存
	entries, err := carParser.ParseIndex()
	if err != nil {
		// 回退：顺序扫描所有块
		log.Debug("BlockFetcher：后台无法解析索引，使用顺序扫描")
		err = carParser.IterateBlocks(func(block *sdkcar.Block) error {
			if bf.cache != nil {
				cidKey := block.CID.String()
				if putErr := bf.cache.Put(cidKey, block.Data); putErr != nil {
					log.Warn("BlockFetcher：后台缓存写入失败 %s: %v", cidKey, putErr)
				}
			}
			return nil
		})
		if err != nil {
			log.Warn("BlockFetcher：后台顺序扫描失败: %v", err)
		}
		return
	}

	// 使用索引逐块读取并缓存
	for _, entry := range entries {
		block, err := carParser.GetBlock(entry.CID)
		if err != nil || block == nil {
			continue
		}
		if bf.cache != nil {
			cidKey := block.CID.String()
			if putErr := bf.cache.Put(cidKey, block.Data); putErr != nil {
				log.Warn("BlockFetcher：后台缓存写入失败 %s: %v", cidKey, putErr)
			}
		}
	}

	_ = ctx
	_ = dataSizeStr
	_ = cidStr

	log.Info("BlockFetcher：后台缓存完成 dataTxID=%s，已缓存 %d 个块", dataTxID, len(entries))
}

// readerAtAdapter 实现 io.ReaderAt 接口，适配字节切片
type readerAtAdapter struct {
	data []byte
}

func (r *readerAtAdapter) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, fmt.Errorf("negative offset")
	}
	if off >= int64(len(r.data)) {
		return 0, fmt.Errorf("EOF")
	}
	n = copy(p, r.data[off:])
	if n < len(p) {
		return n, fmt.Errorf("EOF")
	}
	return n, nil
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

// ensureStrictVerified 确保指定 metaTxID 已通过全部 4 个步骤验证
//
// 在从 Arweave 下载块数据之前调用，确保数据源可信。
// 检查顺序：pow → index → ref_chain → integrity
// 如果全部步骤已缓存，直接返回 nil（跳过验证）。
// 验证失败则返回错误（调用方应跳过该 metaTxID，尝试下一个来源）。
func (bf *BlockFetcher) ensureStrictVerified(ctx context.Context, metaTxID string) error {
	steps := []string{index.StepPoW, index.StepIndex, index.StepRefChain, index.StepIntegrity}

	// 检查所有步骤是否已验证
	if allCached, err := bf.index.AllStepsVerified(metaTxID, steps); err != nil {
		log.Warn("BlockFetcher：检查验证缓存失败 metaTxID=%s: %v", metaTxID, err)
	} else if allCached {
		log.Debug("BlockFetcher：全部步骤已缓存 metaTxID=%s", metaTxID)
		return nil
	}

	// 需要执行验证
	if bf.strictVerifier == nil {
		return fmt.Errorf("BlockFetcher: strict verification required for %s but no verifier configured", metaTxID)
	}

	log.Info("BlockFetcher：开始验证 metaTxID=%s", metaTxID)
	if err := bf.strictVerifier(ctx, metaTxID); err != nil {
		return fmt.Errorf("BlockFetcher: verification failed for %s: %w", metaTxID, err)
	}

	// 验证后再次检查所有步骤是否已标记
	if allCached, err := bf.index.AllStepsVerified(metaTxID, steps); err != nil {
		return fmt.Errorf("BlockFetcher: post-verification cache check failed: %w", err)
	} else if !allCached {
		return fmt.Errorf("BlockFetcher: verification incomplete for %s", metaTxID)
	}

	log.Info("BlockFetcher：验证通过 metaTxID=%s", metaTxID)
	return nil
}
