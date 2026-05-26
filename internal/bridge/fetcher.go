// Package bridge 提供 IPFAR 桥接核心逻辑
//
// 本文件实现 BlockFetcher — 按需从 Arweave 拉取 IPFS 块。
// 用于 Bitswap 按需架构，采用两阶段策略：
//
//	阶段1（立即）: 按需拉 CAR v2 Index → 找到块偏移 → HTTP Range 请求单个块 → 立即回复对手节点
//	阶段2（后台）: 顺序下载完整 CAR 文件 → 验证 → 缓存全部块
//
//	FetchBlock(cid) 流程:
//	  1. 查本地 LRU 缓存 → 命中直接返回
//	  2. 查第一层索引 (CID → metaTxID 列表)
//	  3. 查第二层索引 (metaTxID → 参数: dataTxId, bundleTxId, blockHeight, dataSize)
//	  4. HTTP Range 请求 CAR v2 Index 部分（通常几百 KB）
//	  5. 解析 Index，找到请求的 CID 的 offset
//	  6. HTTP Range 请求单个块（快速回复对手节点）
//	  7. [后台] 顺序下载完整 CAR → 验证 → 缓存所有块
package bridge

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"

	ipfsverify "github.com/LWDJD/ipfar-sdk/verify/ipfs"

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

// MaxBlockRangeSize 单个块 Range 请求的最大下载字节数
// 用于快速路径：请求 offset..offset+MaxBlockRangeSize-1 范围内的数据，
// 然后解析实际块边界。IPFS 默认块大小 256KB，2MB 足以覆盖绝大多数场景。
const MaxBlockRangeSize = 2 << 20 // 2 MB

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

	// 后台完整 CAR 下载跟踪（防止重复下载同一 dataTxID）
	bgDownloads   map[string]chan struct{} // key: dataTxID, value: done channel
	bgDownloadsMu sync.Mutex

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
		bgDownloads:    make(map[string]chan struct{}),
	}

	log.Info("BlockFetcher：初始化完成 max_concurrent=%d", maxConcurrent)
	return bf, nil
}

// FetchBlock 按需获取一个 IPFS 块
//
// 查找链: 缓存 → 第一层索引(CID→metaTxIDs) → 第二层索引(metaTxID→params) →
//
//	按需拉 Index（快速路径）或完整下载（回退路径） → 缓存 → 返回
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

	// Step 3: 查第二层索引：遍历所有 metaTxID，找到第一个可下载的
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

		// ============================================================
		// 阶段1（快速路径）：按需拉 CAR v2 Index → HTTP Range 请求单个块
		// 仅对普通模式尝试快速路径（非跨 Bundle、非同 Bundle 模式）
		// ============================================================
		blockHeight := int64(-1)
		if blockHeightStr != "" {
			fmt.Sscanf(blockHeightStr, "%d", &blockHeight)
		}

		// 跨 Bundle 模式：必须走 bundle 提取路径
		if blockHeight == -1 && bundleTxID != "" && bundleTxID != "none" {
			bf.downloadSem <- struct{}{}
			_, rawData, err := bf.gateway.FetchBundleItemByID(bundleTxID, dataTxID)
			<-bf.downloadSem
			if err != nil {
				lastErr = fmt.Errorf("BlockFetcher: fetch bundle item %s from bundle %s: %w",
					dataTxID, bundleTxID, err)
				continue
			}
			data = rawData
			break
		}

		// 同 Bundle 模式：直接下载完整数据
		if blockHeight == -1 && bundleTxID == "none" {
			bf.downloadSem <- struct{}{}
			data, lastErr = bf.downloadAsRaw(ctx, dataTxID)
			<-bf.downloadSem
			if lastErr == nil && data != nil {
				break
			}
			continue
		}

		// 普通模式：尝试快速路径（按需拉 Index）
		bf.downloadSem <- struct{}{}
		data, lastErr = bf.fetchBlockViaIndex(ctx, dataTxID, cidStr, params)
		<-bf.downloadSem

		if lastErr == nil && data != nil {
			// 快速路径成功！后台启动完整 CAR 下载
			bf.ensureFullCARDownloaded(dataTxID)
			break
		}
		log.Debug("BlockFetcher：按需 Index 路径失败 cid=%s dataTxID=%s: %v，回退到完整下载",
			cidStr, dataTxID, lastErr)

		// 回退：完整下载
		bf.downloadSem <- struct{}{}
		data, lastErr = bf.downloadAsRaw(ctx, dataTxID)
		<-bf.downloadSem

		if lastErr == nil && data != nil {
			bf.ensureFullCARDownloaded(dataTxID)
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

// downloadAsRaw 直接下载交易数据（完整下载整个 Arweave 交易）
func (bf *BlockFetcher) downloadAsRaw(ctx context.Context, txID string) ([]byte, error) {
	log.Debug("BlockFetcher：直接下载交易 %s", txID)
	data, err := bf.gateway.FetchRaw(txID)
	if err != nil {
		return nil, fmt.Errorf("BlockFetcher: download tx %s: %w", txID, err)
	}
	return data, nil
}

// fetchBlockViaIndex 按需拉 CAR v2 Index，找到目标 CID 的偏移量后 Range 下载单个块。
//
// 这是"按需拉 Index"方案的核心实现：
//  1. HTTP Range 读取 CAR v2 头部（前 52 字节），解析 IndexOffset
//  2. HTTP Range 读取 Index 段（通常几百 KB）
//  3. 调用 SDK ParseIndexData 解析 Index，得到 digest→offset 映射
//  4. 从目标 CID 提取 multihash digest，查表获取块偏移量
//  5. HTTP Range 读取块（最多 MaxBlockRangeSize 字节）
//  6. 调用 SDK ExtractBlockFromCar 提取纯数据
//
// 参数 params 来自第二层索引，应包含 "dataSize" 字段。
func (bf *BlockFetcher) fetchBlockViaIndex(ctx context.Context, dataTxID, cidStr string, params map[string]string) ([]byte, error) {
	// 0. 解析目标 CID 的 multihash digest（用于 Index 查表）
	targetCID, err := cid.Decode(cidStr)
	if err != nil {
		return nil, fmt.Errorf("BlockFetcher: decode CID %s: %w", cidStr, err)
	}
	dmh, err := multihash.Decode(targetCID.Hash())
	if err != nil {
		return nil, fmt.Errorf("BlockFetcher: decode multihash for %s: %w", cidStr, err)
	}
	targetDigestHex := hex.EncodeToString(dmh.Digest)

	// 1. 读取 CAR v2 头部（前 52 字节，覆盖 legacy 和 CBOR 两种格式）
	headerData, err := bf.gateway.FetchRange(dataTxID, 0, 52)
	if err != nil {
		return nil, fmt.Errorf("BlockFetcher: read CAR v2 header: %w", err)
	}

	// 检测 pragma 类型，确定 IndexOffset 位置
	var pragmaSize int64
	var v2HeaderSize int64
	switch {
	case len(headerData) >= 4 && string(headerData[:4]) == "car\x02":
		// Legacy: 4 字节 pragma + 48 字节 header
		pragmaSize = 4
		v2HeaderSize = 48
	case len(headerData) >= 11 && headerData[0] == 0x0a:
		// CBOR: 11 字节 pragma + 40 字节 header
		pragmaSize = 11
		v2HeaderSize = 40
	default:
		return nil, fmt.Errorf("BlockFetcher: unknown CAR v2 pragma in %s", dataTxID)
	}

	if int64(len(headerData)) < pragmaSize+v2HeaderSize {
		return nil, fmt.Errorf("BlockFetcher: header data too short: %d < %d",
			len(headerData), pragmaSize+v2HeaderSize)
	}

	v2Header := headerData[pragmaSize : pragmaSize+v2HeaderSize]
	indexOffset := binary.LittleEndian.Uint64(v2Header[32:40])

	if indexOffset == 0 {
		return nil, fmt.Errorf("BlockFetcher: CAR v2 has no index in %s", dataTxID)
	}

	// 计算 IndexSize
	// Legacy 格式: IndexSize 在 header[40:48] 中
	// CBOR 格式: IndexSize = dataSize - IndexOffset（dataSize 来自第二层索引 params）
	var indexSize uint64
	if v2HeaderSize == 48 {
		indexSize = binary.LittleEndian.Uint64(v2Header[40:48])
	}
	if indexSize == 0 {
		// 从 params 获取 dataSize 推算
		if dsStr, ok := params["dataSize"]; ok && dsStr != "" {
			var ds int64
			fmt.Sscanf(dsStr, "%d", &ds)
			if ds > 0 && uint64(ds) > indexOffset {
				indexSize = uint64(ds) - indexOffset
			}
		}
	}
	if indexSize == 0 || indexSize > 10<<20 {
		// Index 为空或过大（超过 10MB 不合理），回退
		return nil, fmt.Errorf("BlockFetcher: invalid index size %d for %s", indexSize, dataTxID)
	}

	log.Debug("BlockFetcher：按需拉取 Index dataTxID=%s indexOffset=%d indexSize=%d",
		dataTxID, indexOffset, indexSize)

	// 2. HTTP Range 读取 Index 段
	indexData, err := bf.gateway.FetchRange(dataTxID, int64(indexOffset), int64(indexSize))
	if err != nil {
		return nil, fmt.Errorf("BlockFetcher: read CAR v2 index: %w", err)
	}

	// 3. 解析 Index → digest→offset 映射
	offsetMap, err := ipfsverify.ParseIndexData(indexData)
	if err != nil {
		return nil, fmt.Errorf("BlockFetcher: parse index data: %w", err)
	}

	// 4. 查表获取块偏移量
	blockOffset, found := offsetMap[targetDigestHex]
	if !found {
		return nil, fmt.Errorf("BlockFetcher: CID %s (digest=%s) not found in index of %s",
			cidStr, targetDigestHex, dataTxID)
	}

	log.Debug("BlockFetcher：Index 命中 cid=%s digest=%s offset=%d", cidStr, targetDigestHex, blockOffset)

	// 5. HTTP Range 读取块（下载 blockOffset 开始的 MaxBlockRangeSize 范围）
	readSize := int64(MaxBlockRangeSize)
	chunk, err := bf.gateway.FetchRange(dataTxID, int64(blockOffset), readSize)
	if err != nil {
		return nil, fmt.Errorf("BlockFetcher: range download block at offset %d: %w", blockOffset, err)
	}

	// 6. 从 chunk 中提取块数据（chunk 的第一个字节就是目标块的 varint 前缀）
	blockData, err := ipfsverify.ExtractBlockFromCar(chunk, 0)
	if err != nil {
		return nil, fmt.Errorf("BlockFetcher: extract block from car data: %w", err)
	}

	log.Info("BlockFetcher：按需 Index 路径成功 cid=%s offset=%d data=%d bytes",
		cidStr, blockOffset, len(blockData))

	return blockData, nil
}

// carBlock 解析出的 CAR 块
type carBlock struct {
	CID  string
	Data []byte
}

// ensureFullCARDownloaded 启动后台任务下载完整 CAR 文件（阶段2）
//
// 使用 map 防止对同一 dataTxID 的重复下载。
// 下载完成后解析 CAR 并将所有块注入 LRU 缓存。
func (bf *BlockFetcher) ensureFullCARDownloaded(dataTxID string) {
	bf.bgDownloadsMu.Lock()
	if _, ok := bf.bgDownloads[dataTxID]; ok {
		bf.bgDownloadsMu.Unlock()
		log.Debug("BlockFetcher：后台下载已在运行 dataTxID=%s", dataTxID)
		return
	}

	// 标记为正在下载
	done := make(chan struct{})
	bf.bgDownloads[dataTxID] = done
	bf.bgDownloadsMu.Unlock()

	go func() {
		defer func() {
			bf.bgDownloadsMu.Lock()
			delete(bf.bgDownloads, dataTxID)
			bf.bgDownloadsMu.Unlock()
			close(done)
		}()

		log.Info("BlockFetcher：后台开始下载完整 CAR dataTxID=%s", dataTxID)

		// 使用独立的 context，不受原始请求超时限制
		bgCtx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()

		// 下载完整 CAR 文件
		rawData, err := bf.gateway.FetchRaw(dataTxID)
		if err != nil {
			log.Warn("BlockFetcher：后台完整 CAR 下载失败 dataTxID=%s: %v", dataTxID, err)
			return
		}

		log.Info("BlockFetcher：后台完整 CAR 下载成功 dataTxID=%s size=%d", dataTxID, len(rawData))

		// 解析 CAR 文件中的块并注入缓存
		blocks, err := bf.parseCARv2Blocks(rawData)
		if err != nil {
			log.Warn("BlockFetcher：后台 CAR 解析失败 dataTxID=%s: %v", dataTxID, err)
		} else {
			for _, block := range blocks {
				if bf.cache != nil {
					if err := bf.cache.Put(block.CID, block.Data); err != nil {
						log.Warn("BlockFetcher：后台缓存写入失败 cid=%s: %v", block.CID, err)
					}
				}
			}
			log.Info("BlockFetcher：后台完整 CAR 已缓存 %d 个块 dataTxID=%s", len(blocks), dataTxID)
		}

		_ = bgCtx
	}()
}

// parseCARv2Blocks 解析 CAR v2 文件中的所有块
// 读取 v2 头部，定位 v1 数据区，遍历所有块
func (bf *BlockFetcher) parseCARv2Blocks(rawData []byte) ([]carBlock, error) {
	if len(rawData) < 52 {
		return nil, fmt.Errorf("data too short for CAR v2")
	}

	var pragmaSize int64
	var v2HeaderSize int64

	// 检测 pragma 类型
	if len(rawData) >= 4 && string(rawData[:4]) == "car\x02" {
		pragmaSize = 4
		v2HeaderSize = 48
	} else if len(rawData) >= 11 && rawData[0] == 0x0a {
		pragmaSize = 11
		v2HeaderSize = 40
	} else {
		return nil, fmt.Errorf("unknown CAR v2 pragma")
	}

	if len(rawData) < int(pragmaSize+v2HeaderSize) {
		return nil, fmt.Errorf("data too short for CAR v2 header")
	}

	v2Header := rawData[pragmaSize : pragmaSize+v2HeaderSize]
	dataOffset := binary.LittleEndian.Uint64(v2Header[16:24])
	dataSize := binary.LittleEndian.Uint64(v2Header[24:32])

	if dataOffset == 0 || dataOffset+dataSize > uint64(len(rawData)) {
		return nil, fmt.Errorf("invalid CAR v2 data offset/size")
	}

	// 读取 CAR v1 数据区
	v1Data := rawData[dataOffset : dataOffset+dataSize]

	// 跳过 CAR v1 头部（version + root_count + root CIDs）
	pos := skipCarV1Header(v1Data)
	if pos < 0 {
		return nil, fmt.Errorf("failed to skip CAR v1 header")
	}

	var blocks []carBlock

	// 扫描所有块
	for pos < len(v1Data) {
		sectionLen, sectionLenLen, err := readVarint(v1Data[pos:])
		if err != nil || sectionLen == 0 {
			break
		}
		if pos+sectionLenLen+int(sectionLen) > len(v1Data) {
			break
		}

		sectionStart := pos + sectionLenLen
		section := v1Data[sectionStart : sectionStart+int(sectionLen)]

		// 从 section 中解析 CID
		cidByteLen, cidStr := parseCIDFromSection(section)
		if cidByteLen <= 0 {
			pos = sectionStart + int(sectionLen)
			continue
		}

		data := section[cidByteLen:]
		blocks = append(blocks, carBlock{CID: cidStr, Data: data})

		pos = sectionStart + int(sectionLen)
	}

	return blocks, nil
}

// skipCarV1Header 跳过 CAR v1 头部，返回数据区起始位置
// 格式: varint(version=1) + varint(root_count) + root_count × (varint(cid_len) + cid_bytes)
func skipCarV1Header(data []byte) int {
	pos := 0

	// version
	_, n, err := readVarint(data[pos:])
	if err != nil || n <= 0 {
		return -1
	}
	pos += n

	// root count
	rootCount, n, err := readVarint(data[pos:])
	if err != nil || n <= 0 {
		return -1
	}
	pos += n

	// skip root CIDs
	for i := uint64(0); i < rootCount; i++ {
		cidLen, n, err := readVarint(data[pos:])
		if err != nil || n <= 0 {
			return -1
		}
		pos += n + int(cidLen)
	}

	return pos
}

// parseCIDFromSection 从 CAR section 数据中解析 CID
// section 格式: CID_bytes + data_bytes
// 返回 (cidByteLen, cidStr)，失败返回 (0, "")
func parseCIDFromSection(section []byte) (int, string) {
	if len(section) < 2 {
		return 0, ""
	}

	version := section[0]
	var cidLen int

	switch version {
	case 0x12: // CID v0 (sha256 multihash prefix, 34 bytes)
		cidLen = 34
	case 0x01: // CID v1
		// <version=1><codec varint><multihash>
		_, codecLen, _ := readVarint(section[1:])
		if codecLen <= 0 {
			return 0, ""
		}
		mhStart := 1 + codecLen
		if mhStart+2 > len(section) {
			return 0, ""
		}
		mhLen := int(section[mhStart+1])
		cidLen = mhStart + 2 + mhLen
	default:
		return 0, ""
	}

	if cidLen > len(section) {
		return 0, ""
	}

	c, err := cid.Cast(section[:cidLen])
	if err != nil {
		return 0, ""
	}
	return cidLen, c.String()
}

// readVarint 从字节切片读取 varint
func readVarint(data []byte) (uint64, int, error) {
	if len(data) == 0 {
		return 0, 0, fmt.Errorf("empty data")
	}
	// Use the stdlib approach: check leading byte
	v := uint64(data[0])
	if v < 0x80 {
		return v, 1, nil
	}
	// Use go-varint for multi-byte (not imported here to keep it simple)
	// Use encoding/binary's varint support
	val, n := binary.Uvarint(data)
	if n <= 0 {
		return 0, 0, fmt.Errorf("invalid varint")
	}
	return val, n, nil
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
