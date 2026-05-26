// Package bridge 提供 IPFAR 桥接核心逻辑
//
// 本文件实现 BlockFetcher — 按需从 Arweave 拉取 IPFS 块。
// 用于 Bitswap 按需架构，采用两阶段策略：
//
//	阶段1（快速路径）: Range 请求 CAR v2 Header → Index → 单个块 → 立即回复
//	阶段1（慢速回退）: 如果 Index 中找不到 CID → 完整下载 CAR → 提取块 → 回复
//	阶段2（后台）: 回复后 → 启动后台 goroutine 下载完整 CAR → 验证 → 缓存所有块
//
//	FetchBlock(cid) 流程:
//	  1. 查本地 LRU 缓存 → 命中直接返回
//	  2. 查第一层索引 (CID → metaTxID 列表)
//	  3. 查第二层索引 (metaTxID → 参数: dataTxId, bundleTxId, blockHeight, dataSize)
//	  4. 调用 fetchBlockViaRange() 尝试快速路径（Range 请求 Index → 解析 → Range 单块）
//	  5. 失败则回退完整下载
//	  6. 写入缓存 → 返回
//	  7. [后台] 启动 ensureFullCARDownloaded() 下载完整 CAR → 验证 → 缓存所有块
//
// 关键原则：不预存偏移。每次请求时从 Arweave 拉 Index 查偏移。
package bridge

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"

	"github.com/lwdjd/IPFAR/internal/cache"
	"github.com/lwdjd/IPFAR/internal/download"
	"github.com/lwdjd/IPFAR/internal/index"
	"github.com/lwdjd/IPFAR/internal/log"
)

// StrictVerifier 严格级别验证器函数类型
type StrictVerifier func(ctx context.Context, metaTxID string) error

// BlockFetcherConfig 按需拉取器配置
type BlockFetcherConfig struct {
	IndexStore             *index.Store
	Cache                  *cache.Cache
	CacheConfig            cache.CacheConfig
	Gateway                *download.Gateway
	MaxConcurrentDownloads int
	Timeout                time.Duration
	StrictVerifier         StrictVerifier
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
const MaxBlockRangeSize = 2 << 20 // 2 MB

// BlockLocation 块在 CAR v2 文件中的位置信息
type BlockLocation struct {
	Offset uint64
	Size   uint64
}

// carV2HeaderInfo CAR v2 头部解析结果
type carV2HeaderInfo struct {
	DataOffset  uint64
	DataSize    uint64
	IndexOffset uint64
	IndexSize   uint64
}

// BlockFetcher 按需拉取 IPFS 块
type BlockFetcher struct {
	config BlockFetcherConfig

	index   *index.Store
	cache   *cache.Cache
	gateway *download.Gateway

	strictVerifier StrictVerifier

	downloadSem chan struct{}

	inFlight   map[string]*inFlightReq
	inFlightMu sync.Mutex

	bgDownloads   map[string]chan struct{}
	bgDownloadsMu sync.Mutex

	mu    sync.RWMutex
	stats BlockFetcherStats
}

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

	// Step 3: 遍历所有 metaTxID
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

		blockHeight := int64(-1)
		if blockHeightStr != "" {
			fmt.Sscanf(blockHeightStr, "%d", &blockHeight)
		}

		// 跨 Bundle 模式
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

		// 同 Bundle 模式
		if blockHeight == -1 && bundleTxID == "none" {
			bf.downloadSem <- struct{}{}
			data, lastErr = bf.downloadAsRaw(ctx, dataTxID)
			<-bf.downloadSem
			if lastErr == nil && data != nil {
				break
			}
			continue
		}

		// 阶段1（快速路径）：按需拉取
		bf.downloadSem <- struct{}{}
		data, lastErr = bf.fetchBlockViaRange(ctx, metaTxID, cidStr)
		<-bf.downloadSem

		if lastErr == nil && data != nil {
			bf.ensureFullCARDownloaded(metaTxID)
			break
		}
		log.Debug("BlockFetcher：快速路径失败 cid=%s metaTxID=%s: %v，回退到完整下载",
			cidStr, metaTxID, lastErr)

		// 回退：完整下载
		bf.downloadSem <- struct{}{}
		data, lastErr = bf.downloadAsRaw(ctx, dataTxID)
		<-bf.downloadSem

		if lastErr == nil && data != nil {
			bf.ensureFullCARDownloaded(metaTxID)
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

// downloadAsRaw 直接下载交易数据
func (bf *BlockFetcher) downloadAsRaw(ctx context.Context, txID string) ([]byte, error) {
	log.Debug("BlockFetcher：直接下载交易 %s", txID)
	data, err := bf.gateway.FetchRaw(txID)
	if err != nil {
		return nil, fmt.Errorf("BlockFetcher: download tx %s: %w", txID, err)
	}
	return data, nil
}

// ============================================================
// 快速路径：fetchBlockViaRange
// ============================================================

// fetchBlockViaRange 通过 HTTP Range 请求按需获取单个 IPFS 块
func (bf *BlockFetcher) fetchBlockViaRange(ctx context.Context, metaTxID, cidStr string) ([]byte, error) {
	params, err := bf.index.GetMeta(metaTxID)
	if err != nil {
		return nil, fmt.Errorf("BlockFetcher: get meta %s: %w", metaTxID, err)
	}
	if params == nil {
		return nil, fmt.Errorf("BlockFetcher: meta %s not found", metaTxID)
	}

	dataTxID := params["dataTxId"]
	if dataTxID == "" {
		return nil, fmt.Errorf("BlockFetcher: no dataTxId for meta %s", metaTxID)
	}

	targetCID, err := cid.Decode(cidStr)
	if err != nil {
		return nil, fmt.Errorf("BlockFetcher: decode CID %s: %w", cidStr, err)
	}
	dmh, err := multihash.Decode(targetCID.Hash())
	if err != nil {
		return nil, fmt.Errorf("BlockFetcher: decode multihash for %s: %w", cidStr, err)
	}
	targetDigestHex := hex.EncodeToString(dmh.Digest)

	// 1. HTTP Range 请求 CAR v2 Header
	headerData, err := bf.gateway.FetchRange(dataTxID, 0, 100)
	if err != nil {
		return nil, fmt.Errorf("BlockFetcher: fetch CAR v2 header: %w", err)
	}

	headerInfo, err := parseCARv2HeaderRange(headerData)
	if err != nil {
		return nil, fmt.Errorf("BlockFetcher: parse CAR v2 header: %w", err)
	}

	if headerInfo.IndexOffset == 0 {
		return nil, fmt.Errorf("BlockFetcher: CAR v2 has no index in %s", dataTxID)
	}

	indexSize := headerInfo.IndexSize
	if indexSize == 0 {
		if dsStr, ok := params["dataSize"]; ok && dsStr != "" {
			var ds int64
			fmt.Sscanf(dsStr, "%d", &ds)
			if ds > 0 && uint64(ds) > headerInfo.IndexOffset {
				indexSize = uint64(ds) - headerInfo.IndexOffset
			}
		}
	}
	if indexSize == 0 || indexSize > 10<<20 {
		return nil, fmt.Errorf("BlockFetcher: invalid index size %d for %s", indexSize, dataTxID)
	}

	log.Debug("BlockFetcher：按需拉取 Index dataTxID=%s indexOffset=%d indexSize=%d",
		dataTxID, headerInfo.IndexOffset, indexSize)

	// 2. HTTP Range 请求 Index 数据
	indexData, err := bf.gateway.FetchRange(dataTxID, int64(headerInfo.IndexOffset), int64(indexSize))
	if err != nil {
		return nil, fmt.Errorf("BlockFetcher: fetch CAR v2 index: %w", err)
	}

	// 3. 解析 Index
	offsetMap, err := parseCARv2IndexRange(indexData)
	if err != nil {
		return nil, fmt.Errorf("BlockFetcher: parse index data: %w", err)
	}

	// 4. 查表
	loc, found := offsetMap[targetDigestHex]
	if !found {
		return nil, fmt.Errorf("BlockFetcher: CID %s (digest=%s) not found in index of %s",
			cidStr, targetDigestHex, dataTxID)
	}

	log.Debug("BlockFetcher：Index 命中 cid=%s digest=%s offset=%d", cidStr, targetDigestHex, loc.Offset)

	// 5. HTTP Range 请求单个块数据
	blockData, err := downloadBlockRange(ctx, bf.gateway, dataTxID, headerInfo.DataOffset+loc.Offset, loc.Size)
	if err != nil {
		return nil, fmt.Errorf("BlockFetcher: download block @%d: %w", loc.Offset, err)
	}

	log.Info("BlockFetcher：快速路径成功 cid=%s offset=%d data=%d bytes",
		cidStr, loc.Offset, len(blockData))

	return blockData, nil
}

// ============================================================
// CAR v2 解析（本地实现）
// ============================================================

// parseCARv2HeaderRange 解析 CAR v2 头部
func parseCARv2HeaderRange(data []byte) (*carV2HeaderInfo, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("header data too short: %d bytes", len(data))
	}

	var pragmaSize int
	var v2HeaderSize int

	switch {
	case len(data) >= 4 && string(data[:4]) == "car\x02":
		pragmaSize = 4
		v2HeaderSize = 48
	case len(data) >= 11 && data[0] == 0x0a:
		pragmaSize = 11
		v2HeaderSize = 40
	default:
		return nil, fmt.Errorf("unknown CAR v2 pragma (first byte: 0x%02x)", data[0])
	}

	if len(data) < pragmaSize+v2HeaderSize {
		return nil, fmt.Errorf("header data too short: have %d, need %d", len(data), pragmaSize+v2HeaderSize)
	}

	v2Header := data[pragmaSize : pragmaSize+v2HeaderSize]

	info := &carV2HeaderInfo{
		DataOffset:  binary.LittleEndian.Uint64(v2Header[16:24]),
		DataSize:    binary.LittleEndian.Uint64(v2Header[24:32]),
		IndexOffset: binary.LittleEndian.Uint64(v2Header[32:40]),
	}

	if v2HeaderSize >= 48 {
		info.IndexSize = binary.LittleEndian.Uint64(v2Header[40:48])
	}

	return info, nil
}

// parseCARv2IndexRange 解析 CAR v2 Index 段（二进制格式）
func parseCARv2IndexRange(data []byte) (map[string]BlockLocation, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("index data too short: %d bytes", len(data))
	}

	codec, n := binary.Uvarint(data)
	if n <= 0 {
		return nil, fmt.Errorf("failed to read index multicodec")
	}
	pos := n

	switch codec {
	case 0x0400: // CarIndexSorted
		return parseIndexSorted(data[pos:])
	case 0x0401: // MultihashIndexSorted
		return parseMultihashIndexSorted(data[pos:])
	default:
		return nil, fmt.Errorf("unsupported index codec: 0x%x", codec)
	}
}

// parseIndexSorted 解析 CarIndexSorted 格式 (0x0400)
func parseIndexSorted(data []byte) (map[string]BlockLocation, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("CarIndexSorted data too short")
	}

	pos := 0
	bucketCount := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	if bucketCount < 0 || bucketCount > 100000 {
		return nil, fmt.Errorf("unreasonable bucket count: %d", bucketCount)
	}

	result := make(map[string]BlockLocation)

	for i := 0; i < bucketCount; i++ {
		if pos+12 > len(data) {
			return nil, fmt.Errorf("bucket %d data out of bounds", i)
		}

		width := binary.LittleEndian.Uint32(data[pos:])
		pos += 4
		dataLen := binary.LittleEndian.Uint64(data[pos:])
		pos += 8

		if width < 8 {
			return nil, fmt.Errorf("bucket %d width too small: %d", i, width)
		}

		if pos+int(dataLen) > len(data) {
			return nil, fmt.Errorf("bucket %d data length out of bounds", i)
		}

		digestLen := int(width) - 8
		entryWidth := int(width)
		entryCount := int(dataLen) / entryWidth

		for j := 0; j < entryCount; j++ {
			entryStart := pos + j*entryWidth
			digest := data[entryStart : entryStart+digestLen]
			offset := binary.LittleEndian.Uint64(data[entryStart+digestLen : entryStart+digestLen+8])

			key := hex.EncodeToString(digest)
			result[key] = BlockLocation{Offset: offset, Size: 0}
		}

		pos += int(dataLen)
	}

	return result, nil
}

// parseMultihashIndexSorted 解析 MultihashIndexSorted 格式 (0x0401)
func parseMultihashIndexSorted(data []byte) (map[string]BlockLocation, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("MultihashIndexSorted data too short")
	}

	pos := 0
	bucketCount := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	if bucketCount < 0 || bucketCount > 100000 {
		return nil, fmt.Errorf("unreasonable bucket count: %d", bucketCount)
	}

	result := make(map[string]BlockLocation)

	for i := 0; i < bucketCount; i++ {
		if pos+8 > len(data) {
			return nil, fmt.Errorf("multihash bucket %d data out of bounds", i)
		}

		mhCode := binary.LittleEndian.Uint64(data[pos:])
		pos += 8

		entries, newPos, err := parseMultiWidthIndex(data, pos, mhCode)
		if err != nil {
			return nil, fmt.Errorf("multihash bucket %d (code=%d): %w", i, mhCode, err)
		}
		pos = newPos
		for k, v := range entries {
			result[k] = v
		}
	}

	return result, nil
}

// parseMultiWidthIndex 解析 multiWidthIndex
func parseMultiWidthIndex(data []byte, startPos int, mhCode uint64) (map[string]BlockLocation, int, error) {
	pos := startPos

	if pos+4 > len(data) {
		return nil, pos, fmt.Errorf("multiWidthIndex data too short")
	}

	bucketCount := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	result := make(map[string]BlockLocation)

	for i := 0; i < bucketCount; i++ {
		if pos+12 > len(data) {
			return nil, pos, fmt.Errorf("width bucket %d out of bounds", i)
		}

		width := binary.LittleEndian.Uint32(data[pos:])
		pos += 4
		dataLen := binary.LittleEndian.Uint64(data[pos:])
		pos += 8

		if width < 8 {
			return nil, pos, fmt.Errorf("width bucket %d width too small: %d", i, width)
		}

		if pos+int(dataLen) > len(data) {
			return nil, pos, fmt.Errorf("width bucket %d data out of bounds", i)
		}

		digestLen := int(width) - 8
		entryWidth := int(width)
		entryCount := int(dataLen) / entryWidth

		for j := 0; j < entryCount; j++ {
			entryStart := pos + j*entryWidth
			digest := data[entryStart : entryStart+digestLen]
			offset := binary.LittleEndian.Uint64(data[entryStart+digestLen : entryStart+digestLen+8])

			mh, err := multihash.Encode(digest, mhCode)
			if err != nil {
				continue
			}

			key := hex.EncodeToString(mh)
			result[key] = BlockLocation{Offset: offset, Size: 0}
		}

		pos += int(dataLen)
	}

	return result, pos, nil
}

// ============================================================
// 块下载
// ============================================================

// downloadBlockRange 通过 HTTP Range 请求下载单个块，提取纯数据
func downloadBlockRange(ctx context.Context, gateway *download.Gateway, dataTxID string, absoluteOffset uint64, sizeHint uint64) ([]byte, error) {
	readSize := int64(MaxBlockRangeSize)
	if sizeHint > 0 && sizeHint < MaxBlockRangeSize {
		readSize = int64(sizeHint) + 256
	}

	chunk, err := gateway.FetchRange(dataTxID, int64(absoluteOffset), readSize)
	if err != nil {
		return nil, fmt.Errorf("range download at offset %d: %w", absoluteOffset, err)
	}

	if len(chunk) < 2 {
		return nil, fmt.Errorf("downloaded chunk too short at offset %d: %d bytes", absoluteOffset, len(chunk))
	}

	sectionLen, varintLen := binary.Uvarint(chunk)
	if varintLen <= 0 || sectionLen == 0 {
		return nil, fmt.Errorf("failed to read section varint at offset %d", absoluteOffset)
	}

	sectionStart := varintLen
	sectionEnd := sectionStart + int(sectionLen)
	if sectionEnd > len(chunk) {
		return nil, fmt.Errorf("section extends beyond chunk: sectionLen=%d chunkLen=%d", sectionLen, len(chunk))
	}

	section := chunk[sectionStart:sectionEnd]

	cidByteLen, _, err := parseCIDFromSectionBytes(section)
	if err != nil {
		return nil, fmt.Errorf("parse CID from section: %w", err)
	}

	if cidByteLen >= len(section) {
		return nil, fmt.Errorf("CID length %d exceeds section length %d", cidByteLen, len(section))
	}

	pureData := section[cidByteLen:]

	log.Debug("BlockFetcher：downloadBlockRange offset=%d sectionLen=%d cidLen=%d dataLen=%d",
		absoluteOffset, sectionLen, cidByteLen, len(pureData))

	return pureData, nil
}

// parseCIDFromSectionBytes 从 CAR section 字节中解析 CID
func parseCIDFromSectionBytes(section []byte) (int, string, error) {
	if len(section) < 2 {
		return 0, "", fmt.Errorf("section too short: %d bytes", len(section))
	}

	version := section[0]
	var cidLen int

	switch version {
	case 0x12:
		cidLen = 34
	case 0x01:
		_, codecLen, _ := readVarintU64(section[1:])
		if codecLen <= 0 {
			return 0, "", fmt.Errorf("failed to read CID v1 codec")
		}
		mhStart := 1 + codecLen
		if mhStart+2 > len(section) {
			return 0, "", fmt.Errorf("section too short for multihash header")
		}
		mhLen := int(section[mhStart+1])
		cidLen = mhStart + 2 + mhLen
	default:
		return 0, "", fmt.Errorf("unknown CID version byte: 0x%02x", version)
	}

	if cidLen > len(section) {
		return 0, "", fmt.Errorf("CID length %d exceeds section length %d", cidLen, len(section))
	}

	c, err := cid.Cast(section[:cidLen])
	if err != nil {
		return 0, "", fmt.Errorf("invalid CID bytes: %w", err)
	}

	return cidLen, c.String(), nil
}

func readVarintU64(data []byte) (uint64, int, error) {
	return readVarint(data)
}

// ============================================================
// 后台完整 CAR 下载（阶段2）
// ============================================================

type carBlock struct {
	CID  string
	Data []byte
}

// ensureFullCARDownloaded 启动后台任务下载完整 CAR 文件
func (bf *BlockFetcher) ensureFullCARDownloaded(metaTxID string) {
	params, err := bf.index.GetMeta(metaTxID)
	if err != nil || params == nil {
		log.Warn("BlockFetcher：后台下载无法获取 meta %s: %v", metaTxID, err)
		return
	}
	dataTxID := params["dataTxId"]
	if dataTxID == "" {
		log.Warn("BlockFetcher：后台下载 meta %s 无 dataTxId", metaTxID)
		return
	}

	bf.bgDownloadsMu.Lock()
	if _, ok := bf.bgDownloads[dataTxID]; ok {
		bf.bgDownloadsMu.Unlock()
		log.Debug("BlockFetcher：后台下载已在运行 dataTxID=%s", dataTxID)
		return
	}

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

		log.Info("BlockFetcher：后台开始下载完整 CAR metaTxID=%s dataTxID=%s", metaTxID, dataTxID)

		bgCtx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()

		rawData, err := bf.gateway.FetchRaw(dataTxID)
		if err != nil {
			log.Warn("BlockFetcher：后台完整 CAR 下载失败 dataTxID=%s: %v", dataTxID, err)
			return
		}

		log.Info("BlockFetcher：后台完整 CAR 下载成功 dataTxID=%s size=%d", dataTxID, len(rawData))

		blocks, err := bf.parseCARv2Blocks(rawData)
		if err != nil {
			log.Warn("BlockFetcher：后台 CAR 解析失败 dataTxID=%s: %v", dataTxID, err)
		} else {
			for _, block := range blocks {
				if bf.cache != nil {
					if err := bf.cache.Put(block.CID, block.Data); err != nil {
						if err == cache.ErrCacheFull {
							log.Warn("BlockFetcher：后台缓存写入失败 cid=%s: 缓存满且无可驱逐条目", block.CID)
						} else {
							log.Warn("BlockFetcher：后台缓存写入失败 cid=%s: %v", block.CID, err)
						}
					}
				}
			}
			log.Info("BlockFetcher：后台完整 CAR 已缓存 %d 个块 dataTxID=%s", len(blocks), dataTxID)
		}

		_ = bgCtx
	}()
}

// parseCARv2Blocks 解析 CAR v2 文件中的所有块
func (bf *BlockFetcher) parseCARv2Blocks(rawData []byte) ([]carBlock, error) {
	if len(rawData) < 52 {
		return nil, fmt.Errorf("data too short for CAR v2")
	}

	headerInfo, err := parseCARv2HeaderRange(rawData)
	if err != nil {
		return nil, fmt.Errorf("parse CAR v2 header: %w", err)
	}

	dataOffset := headerInfo.DataOffset
	dataSize := headerInfo.DataSize

	if dataOffset == 0 || dataOffset+dataSize > uint64(len(rawData)) {
		return nil, fmt.Errorf("invalid CAR v2 data offset/size")
	}

	v1Data := rawData[dataOffset : dataOffset+dataSize]

	pos := skipCarV1Header(v1Data)
	if pos < 0 {
		return nil, fmt.Errorf("failed to skip CAR v1 header")
	}

	var blocks []carBlock

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

		cidByteLen, cidStr, _ := parseCIDFromSectionBytes(section)
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

// skipCarV1Header 跳过 CAR v1 头部
func skipCarV1Header(data []byte) int {
	pos := 0

	_, n, err := readVarint(data[pos:])
	if err != nil || n <= 0 {
		return -1
	}
	pos += n

	rootCount, n, err := readVarint(data[pos:])
	if err != nil || n <= 0 {
		return -1
	}
	pos += n

	for i := uint64(0); i < rootCount; i++ {
		cidLen, n, err := readVarint(data[pos:])
		if err != nil || n <= 0 {
			return -1
		}
		pos += n + int(cidLen)
	}

	return pos
}

// readVarint 从字节切片读取 varint
func readVarint(data []byte) (uint64, int, error) {
	if len(data) == 0 {
		return 0, 0, fmt.Errorf("empty data")
	}
	if data[0] < 0x80 {
		return uint64(data[0]), 1, nil
	}
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
func (bf *BlockFetcher) ensureStrictVerified(ctx context.Context, metaTxID string) error {
	steps := []string{index.StepPoW, index.StepIndex, index.StepRefChain, index.StepIntegrity}

	if allCached, err := bf.index.AllStepsVerified(metaTxID, steps); err != nil {
		log.Warn("BlockFetcher：检查验证缓存失败 metaTxID=%s: %v", metaTxID, err)
	} else if allCached {
		log.Debug("BlockFetcher：全部步骤已缓存 metaTxID=%s", metaTxID)
		return nil
	}

	if bf.strictVerifier == nil {
		return fmt.Errorf("BlockFetcher: strict verification required for %s but no verifier configured", metaTxID)
	}

	log.Info("BlockFetcher：开始验证 metaTxID=%s", metaTxID)
	if err := bf.strictVerifier(ctx, metaTxID); err != nil {
		return fmt.Errorf("BlockFetcher: verification failed for %s: %w", metaTxID, err)
	}

	if allCached, err := bf.index.AllStepsVerified(metaTxID, steps); err != nil {
		return fmt.Errorf("BlockFetcher: post-verification cache check failed: %w", err)
	} else if !allCached {
		return fmt.Errorf("BlockFetcher: verification incomplete for %s", metaTxID)
	}

	log.Info("BlockFetcher：验证通过 metaTxID=%s", metaTxID)
	return nil
}

var _ = sort.Ints
