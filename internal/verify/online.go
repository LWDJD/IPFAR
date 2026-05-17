// Package verify 提供在线验证功能，通过 HTTP Range 请求对 CAR v2 文件
// 进行随机采样 CID 验证，无需下载完整文件。
//
// 核心思路：
// CAR v2 文件自带 Index（索引段），索引里记录了每个 block 的 Offset 和 CID。
// 这意味着不需要下载整个文件就能随机读取任意 block——发一个 HTTP Range
// 请求直接从 Arweave 网关拿到指定 block 的数据。
//
// 路径 A：在线验证（不存盘）
//   1. 从元数据获取 data_txid
//   2. 用 HTTP Range 请求读取 CAR v2 的 Index 段
//   3. 从 Index 中随机抽取部分 block 进行 CID 验证
//   4. 验证通过就完事了——不下载整个文件
//   5. 验证结果记录到元数据索引（Badger/JSONL）里
package verify

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync"
	"time"

	sdkmeta "github.com/LWDJD/ipfar-sdk/verify/metadata"
	"github.com/LWDJD/ipfar-sdk/verify/ipfs"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/multiformats/go-varint"

	"github.com/lwdjd/IPFAR/internal/download"
	"github.com/lwdjd/IPFAR/internal/log"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// OnlineVerifierConfig 在线验证器配置
type OnlineVerifierConfig struct {
	// Gateway 网关客户端（若为 nil 则使用默认网关）
	Gateway *download.Gateway
	// SampleCount 随机采样 block 数量（默认 5）
	SampleCount int
	// MaxConcurrency 最大并发数（默认 4）
	MaxConcurrency int
}

// DefaultOnlineVerifierConfig 返回默认在线验证器配置
func DefaultOnlineVerifierConfig() OnlineVerifierConfig {
	return OnlineVerifierConfig{
		Gateway:        nil, // 延迟初始化
		SampleCount:    5,
		MaxConcurrency: 4,
	}
}

// OnlineVerifier 在线验证器
// 通过 HTTP Range 请求对远程 CAR v2 文件进行随机采样 CID 验证
type OnlineVerifier struct {
	config  OnlineVerifierConfig
	gateway *download.Gateway
	sema    chan struct{} // 并发控制信号量

	// Bundle 数据缓存（同 Bundle 验证使用）
	mu              sync.RWMutex
	bundleDataCache map[string][]byte // key: itemID, value: raw CAR data
}

// NewOnlineVerifier 创建新的在线验证器
func NewOnlineVerifier(config OnlineVerifierConfig) *OnlineVerifier {
	if config.Gateway != nil {
		if config.SampleCount <= 0 {
			config.SampleCount = 5
		}
		if config.MaxConcurrency <= 0 {
			config.MaxConcurrency = 4
		}
		return &OnlineVerifier{
			config:  config,
			gateway: config.Gateway,
			sema:    make(chan struct{}, config.MaxConcurrency),
		}
	}

	cfg := OnlineVerifierConfig{
		Gateway:        download.NewGateway(download.DefaultGatewayConfig()),
		SampleCount:    config.SampleCount,
		MaxConcurrency: config.MaxConcurrency,
	}
	if cfg.SampleCount <= 0 {
		cfg.SampleCount = 5
	}
	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = 4
	}

	return &OnlineVerifier{
		config:  cfg,
		gateway: cfg.Gateway,
		sema:    make(chan struct{}, cfg.MaxConcurrency),
	}
}

// IndexEntry CAR v2 索引条目：multihash digest → CAR v1 data 区内的偏移量
type IndexEntry struct {
	MultihashDigest []byte // multihash 的 digest 部分
	Offset          uint64 // 在 CAR v1 data payload 中的偏移量
}

// OnlineVerifyResult 在线验证结果
type OnlineVerifyResult struct {
	// Passed 是否全部采样 block 通过验证
	Passed bool
	// TotalBlocks 索引中的总 block 数
	TotalBlocks int
	// SampledBlocks 实际采样的 block 数
	SampledBlocks int
	// VerifiedBlocks 通过 CID 验证的 block 数
	VerifiedBlocks int
	// FailedBlocks 未通过 CID 验证的 block 数
	FailedBlocks int
	// Errors 遇到的错误
	Errors []string
	// DataTXID 验证的 data_txid
	DataTXID string
	// IndexSize 索引段大小（字节）
	IndexSize uint64
}

// Verify 通过 data_txid 执行在线验证
//
// 流程：
//  1. HEAD 请求获取文件总大小
//  2. 用 HTTP Range 请求读取 CAR v2 头部（前 51 字节）
//  3. 从头部 + 文件大小计算 IndexOffset, IndexSize
//  4. 用 HTTP Range 请求读取 Index 段
//  5. 解析索引条目
//  6. 随机采样 block
//  7. 并发下载采样 block 并验证 CID
func (v *OnlineVerifier) Verify(dataTXID string) (*OnlineVerifyResult, error) {
	log.Info("在线验证：开始 data_txid=%s", dataTXID)

	result := &OnlineVerifyResult{
		DataTXID: dataTXID,
	}

	// Step 0: HEAD 请求获取文件大小（用于计算 IndexSize）
	fileSize, err := v.fetchFileSize(dataTXID)
	if err != nil {
		log.Warn("在线验证：获取文件大小失败，尝试盲猜: %v", err)
		// 继续执行：尝试用 Range 请求读取尾部
		fileSize = 0
	}

	// Step 1: 读取 CAR v2 头部（前 51 字节：Pragma 11 + Header 40）
	headerData, err := v.fetchRange(dataTXID, 0, 51)
	if err != nil {
		return nil, fmt.Errorf("读取 CAR v2 头部失败: %w", err)
	}

	// 解析 CAR v2 版本
	version, err := v.readVersion(headerData)
	if err != nil {
		return nil, fmt.Errorf("解析 CAR 版本失败: %w", err)
	}
	if version != 2 {
		return nil, fmt.Errorf("仅支持 CAR v2 格式，当前版本: %d", version)
	}

	// 解析 CAR v2 header
	v2Header, err := v.parseCarV2Header(headerData)
	if err != nil {
		return nil, fmt.Errorf("解析 CAR v2 头部失败: %w", err)
	}

	// 使用文件大小计算准确的 IndexSize
	if fileSize > 0 {
		v2Header.IndexSize = uint64(fileSize) - v2Header.IndexOffset
	} else {
		// 盲猜：尝试读取末尾 1KB 看是否有 index
		v2Header.IndexSize = 65536 // 先假设 64KB index
	}

	if !v2Header.HasIndex() || v2Header.IndexSize == 0 {
		return nil, fmt.Errorf("CAR v2 文件不包含索引段")
	}

	result.IndexSize = v2Header.IndexSize

	// Step 2: 读取 Index 段
	indexData, err := v.fetchRange(dataTXID, int64(v2Header.IndexOffset), int64(v2Header.IndexSize))
	if err != nil {
		return nil, fmt.Errorf("读取 Index 段失败: %w", err)
	}

	// Step 3: 解析索引条目
	entries, err := v.parseIndex(indexData)
	if err != nil {
		return nil, fmt.Errorf("解析索引失败: %w", err)
	}

	result.TotalBlocks = len(entries)
	log.Info("在线验证：索引包含 %d 个 block，索引大小=%d bytes", result.TotalBlocks, result.IndexSize)

	if len(entries) == 0 {
		result.Passed = true
		return result, nil
	}

	// Step 4: 随机采样
	sampleCount := v.config.SampleCount
	if sampleCount > len(entries) {
		sampleCount = len(entries)
	}
	sampled := v.randomSample(entries, sampleCount)
	result.SampledBlocks = len(sampled)

	log.Info("在线验证：随机采样 %d/%d 个 block", result.SampledBlocks, result.TotalBlocks)

	// Step 5: 并发验证采样 block
	type verifyResult struct {
		index  int
		passed bool
		err    error
	}

	var wg sync.WaitGroup
	results := make(chan verifyResult, len(sampled))

	for i, entry := range sampled {
		wg.Add(1)
		go func(idx int, e IndexEntry) {
			defer wg.Done()

			// 获取信号量
			v.sema <- struct{}{}
			defer func() { <-v.sema }()

			passed, err := v.verifyBlock(dataTXID, v2Header, e)
			results <- verifyResult{index: idx, passed: passed, err: err}
		}(i, entry)
	}

	wg.Wait()
	close(results)

	// 汇总结果
	for res := range results {
		if res.err != nil {
			result.Errors = append(result.Errors, res.err.Error())
			result.FailedBlocks++
		} else if res.passed {
			result.VerifiedBlocks++
		} else {
			result.FailedBlocks++
		}
	}

	result.Passed = result.FailedBlocks == 0 && result.VerifiedBlocks > 0

	if result.Passed {
		log.Info("在线验证：通过 ✅ data_txid=%s verified=%d/%d",
			dataTXID, result.VerifiedBlocks, result.SampledBlocks)
	} else {
		log.Warn("在线验证：失败 ❌ data_txid=%s verified=%d failed=%d errors=%d",
			dataTXID, result.VerifiedBlocks, result.FailedBlocks, len(result.Errors))
	}

	return result, nil
}

// VerifyWithMeta 根据完整的元数据选择正确的验证路径
//
// 自动处理三种模式：
//  1. 普通模式（data_height >= 0）：直接验证 data_txid
//  2. 跨 Bundle 模式（data_height = -1, bundle_txid != "none"）：
//     解析 Bundle 头部获取 Item 偏移量，通过 Range 请求验证
//  3. 同 Bundle 模式（data_height = -1, bundle_txid = "none"）：
//     需要预先缓存 Bundle raw data（通过 SetBundleData 注入）
func (v *OnlineVerifier) VerifyWithMeta(meta *sdkmeta.Metadata) (*OnlineVerifyResult, error) {
	if meta == nil {
		return nil, fmt.Errorf("metadata is nil")
	}

	if meta.IsCrossBundle() {
		return v.verifyBundleItem(meta.BundleTXID, meta.DataTXID)
	} else if meta.IsSameBundle() {
		return v.verifySameBundleItem(meta.DataTXID)
	}

	// 普通模式
	return v.Verify(meta.DataTXID)
}

// verifyBundleItem 验证跨 Bundle 中的 Item
// 通过解析 Bundle 头部获取 Item 偏移量，然后用 Range 请求验证
func (v *OnlineVerifier) verifyBundleItem(bundleTXID, itemID string) (*OnlineVerifyResult, error) {
	log.Info("在线验证：跨 Bundle 模式 bundle=%s item=%s", bundleTXID, itemID)

	// Step 1: 获取 Bundle 头部，找到 Item 的偏移和长度
	_, bundleHeader, err := v.gateway.FetchBundleItemByID(bundleTXID, itemID)
	if err != nil {
		return nil, fmt.Errorf("在线验证：获取 Bundle Item 失败: %w", err)
	}

	// bundleHeader 是纯 CAR 数据（不含 ANS-104 头部）
	// 需要将纯数据包装为可验证的格式
	// 这里我们直接将数据写入临时缓冲区进行验证

	result := &OnlineVerifyResult{
		DataTXID: itemID,
	}

	if len(bundleHeader) == 0 {
		return nil, fmt.Errorf("在线验证：Bundle Item 数据为空")
	}

	// Step 2: 使用内存中的数据进行验证
	// 创建 bytes.Reader 用于验证
	reader := newBytesReader(bundleHeader)

	// 解析 CAR v2 版本
	version, err := v.readVersion(bundleHeader)
	if err != nil {
		return nil, fmt.Errorf("解析 CAR 版本失败: %w", err)
	}
	if version != 2 {
		return nil, fmt.Errorf("仅支持 CAR v2 格式，当前版本: %d", version)
	}

	// 解析 CAR v2 header
	v2Header, err := v.parseCarV2Header(bundleHeader)
	if err != nil {
		return nil, fmt.Errorf("解析 CAR v2 头部失败: %w", err)
	}

	_ = reader // silence unused warning

	// 计算准确的 IndexSize
	fileSize := int64(len(bundleHeader))
	v2Header.IndexSize = uint64(fileSize) - v2Header.IndexOffset

	if !v2Header.HasIndex() || v2Header.IndexSize == 0 {
		return nil, fmt.Errorf("CAR v2 文件不包含索引段")
	}

	result.IndexSize = v2Header.IndexSize

	// 读取 Index 段
	if int(v2Header.IndexOffset)+int(v2Header.IndexSize) > len(bundleHeader) {
		return nil, fmt.Errorf("索引段越界")
	}
	indexData := bundleHeader[v2Header.IndexOffset : v2Header.IndexOffset+v2Header.IndexSize]

	// 解析索引条目
	entries, err := v.parseIndex(indexData)
	if err != nil {
		return nil, fmt.Errorf("解析索引失败: %w", err)
	}

	result.TotalBlocks = len(entries)
	log.Info("在线验证（跨 Bundle）：索引包含 %d 个 block", result.TotalBlocks)

	if len(entries) == 0 {
		result.Passed = true
		return result, nil
	}

	// 采样验证
	sampleCount := v.config.SampleCount
	if sampleCount > len(entries) {
		sampleCount = len(entries)
	}
	sampled := v.randomSample(entries, sampleCount)
	result.SampledBlocks = len(sampled)

	// 并发验证采样 block
	var wg sync.WaitGroup
	results := make(chan struct {
		passed bool
		err    error
	}, len(sampled))

	for _, entry := range sampled {
		wg.Add(1)
		go func(e IndexEntry) {
			defer wg.Done()
			v.sema <- struct{}{}
			defer func() { <-v.sema }()

			passed, err := v.verifyBlockInMemory(bundleHeader, v2Header, e)
			results <- struct {
				passed bool
				err    error
			}{passed, err}
		}(entry)
	}

	wg.Wait()
	close(results)

	for res := range results {
		if res.err != nil {
			result.Errors = append(result.Errors, res.err.Error())
			result.FailedBlocks++
		} else if res.passed {
			result.VerifiedBlocks++
		} else {
			result.FailedBlocks++
		}
	}

	result.Passed = result.FailedBlocks == 0 && result.VerifiedBlocks > 0
	return result, nil
}

// verifySameBundleItem 验证同 Bundle 中的 Item
// 需要预先通过 SetBundleData 缓存 Bundle raw data
func (v *OnlineVerifier) verifySameBundleItem(itemID string) (*OnlineVerifyResult, error) {
	v.mu.RLock()
	bundleData, ok := v.bundleDataCache[itemID]
	v.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("同 Bundle 数据未缓存: item_id=%s，请先调用 SetBundleData", itemID)
	}

	// 复用 verifyBundleItem 的逻辑（bundleData 已经是纯 CAR 数据）
	log.Info("在线验证：同 Bundle 模式 item=%s", itemID)

	result := &OnlineVerifyResult{
		DataTXID: itemID,
	}

	return v.verifyFromBytes(bundleData, result)
}

// verifyBlockInMemory 在内存中验证单个 block（无需 HTTP 请求）
func (v *OnlineVerifier) verifyBlockInMemory(data []byte, hdr *carV2Header, entry IndexEntry) (bool, error) {
	fileOffset := int64(hdr.DataOffset) + int64(entry.Offset)

	if fileOffset+256 > int64(len(data)) {
		return false, fmt.Errorf("block @%d 越界", fileOffset)
	}

	headData := data[fileOffset:]

	// 解析 section length
	br := newBytesReader(headData)
	sectionLen, err := varint.ReadUvarint(br)
	if err != nil {
		return false, fmt.Errorf("解析 block section length @%d 失败: %w", fileOffset, err)
	}

	if sectionLen == 0 {
		return false, fmt.Errorf("block section length 为 0 @%d", fileOffset)
	}

	cidStart := int64(br.pos)
	cidEnd := cidStart + int64(sectionLen)

	if cidEnd > int64(len(headData)) {
		return false, fmt.Errorf("block CID 越界 @%d", fileOffset)
	}

	_, blockCID, err := cid.CidFromBytes(headData[cidStart:cidEnd])
	if err != nil {
		return false, fmt.Errorf("解析 block CID @%d 失败: %w", fileOffset, err)
	}

	dataStart := cidStart + int64(blockCID.ByteLen())
	dataEnd := int64(br.pos) + int64(sectionLen)
	blockData := headData[dataStart:dataEnd]

	hash, err := blockCID.Prefix().Sum(blockData)
	if err != nil {
		return false, fmt.Errorf("计算 block @%d hash 失败: %w", fileOffset, err)
	}

	if !hash.Equals(blockCID) {
		return false, fmt.Errorf("block @%d CID 不匹配", fileOffset)
	}

	return true, nil
}

// verifyFromBytes 从内存中的 CAR 数据进行验证
func (v *OnlineVerifier) verifyFromBytes(data []byte, result *OnlineVerifyResult) (*OnlineVerifyResult, error) {
	if len(data) < 51 {
		return nil, fmt.Errorf("CAR 数据太短")
	}

	version, err := v.readVersion(data)
	if err != nil || version != 2 {
		return nil, fmt.Errorf("不是有效的 CAR v2 文件")
	}

	v2Header, err := v.parseCarV2Header(data)
	if err != nil {
		return nil, err
	}

	fileSize := int64(len(data))
	v2Header.IndexSize = uint64(fileSize) - v2Header.IndexOffset

	if !v2Header.HasIndex() || v2Header.IndexSize == 0 {
		return nil, fmt.Errorf("CAR v2 文件不包含索引段")
	}

	result.IndexSize = v2Header.IndexSize

	if int(v2Header.IndexOffset)+int(v2Header.IndexSize) > len(data) {
		return nil, fmt.Errorf("索引段越界")
	}
	indexData := data[v2Header.IndexOffset : v2Header.IndexOffset+v2Header.IndexSize]

	entries, err := v.parseIndex(indexData)
	if err != nil {
		return nil, err
	}

	result.TotalBlocks = len(entries)

	if len(entries) == 0 {
		result.Passed = true
		return result, nil
	}

	sampleCount := v.config.SampleCount
	if sampleCount > len(entries) {
		sampleCount = len(entries)
	}
	sampled := v.randomSample(entries, sampleCount)
	result.SampledBlocks = len(sampled)

	var wg sync.WaitGroup
	results := make(chan struct {
		passed bool
		err    error
	}, len(sampled))

	for _, entry := range sampled {
		wg.Add(1)
		go func(e IndexEntry) {
			defer wg.Done()
			v.sema <- struct{}{}
			defer func() { <-v.sema }()

			passed, err := v.verifyBlockInMemory(data, v2Header, e)
			results <- struct {
				passed bool
				err    error
			}{passed, err}
		}(entry)
	}

	wg.Wait()
	close(results)

	for res := range results {
		if res.err != nil {
			result.Errors = append(result.Errors, res.err.Error())
			result.FailedBlocks++
		} else if res.passed {
			result.VerifiedBlocks++
		} else {
			result.FailedBlocks++
		}
	}

	result.Passed = result.FailedBlocks == 0 && result.VerifiedBlocks > 0
	return result, nil
}

// SetBundleData 缓存 Bundle raw data 用于同 Bundle 验证
func (v *OnlineVerifier) SetBundleData(itemID string, data []byte) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.bundleDataCache == nil {
		v.bundleDataCache = make(map[string][]byte)
	}
	v.bundleDataCache[itemID] = data
}
func (v *OnlineVerifier) VerifyWithSampleCount(dataTXID string, sampleCount int) (*OnlineVerifyResult, error) {
	oldCount := v.config.SampleCount
	v.config.SampleCount = sampleCount
	defer func() { v.config.SampleCount = oldCount }()
	return v.Verify(dataTXID)
}

// carV2Header CAR v2 头部解析结果
type carV2Header struct {
	DataOffset  uint64
	DataSize    uint64
	IndexOffset uint64
	IndexSize   uint64
}

// HasIndex 判断是否有索引
// 只要 IndexOffset > 0 就认为有索引（IndexSize 可从文件大小计算）
func (h *carV2Header) HasIndex() bool {
	return h.IndexOffset > 0
}

// readVersion 从 CAR 文件头部读取版本号
func (v *OnlineVerifier) readVersion(data []byte) (uint64, error) {
	if len(data) < 11 {
		return 0, fmt.Errorf("数据太短，无法读取 CAR 版本")
	}

	// CAR v2 pragma: 0x0aa16776657273696f6e02
	// 是一个有效的 CARv1 header (CBOR)，version=2
	if len(data) >= 4 && data[0] == 0x0a && data[1] == 0xa1 &&
		data[2] == 0x67 && data[3] == 0x76 {
		// 看起来像 CAR v2 pragma，验证完整 pragma
		if len(data) >= 11 {
			pragma := data[:11]
			if pragma[0] == 0x0a && pragma[1] == 0xa1 && pragma[2] == 0x67 &&
				pragma[3] == 0x76 && pragma[4] == 0x65 && pragma[5] == 0x72 &&
				pragma[6] == 0x73 && pragma[7] == 0x69 && pragma[8] == 0x6f &&
				pragma[9] == 0x6e && pragma[10] == 0x02 {
				return 2, nil
			}
		}
	}

	// 可能是 CAR v1
	return varint.ReadUvarint(newBytesReader(data))
}

// parseCarV2Header 解析 CAR v2 头部（Pragma 后的 40 字节）
func (v *OnlineVerifier) parseCarV2Header(data []byte) (*carV2Header, error) {
	// Pragma 11 bytes + Header 40 bytes = 51 bytes
	if len(data) < 51 {
		return nil, fmt.Errorf("数据太短，无法解析 CAR v2 header (需要 51 字节，实际 %d)", len(data))
	}

	// 跳过 Pragma (11 bytes)，读取 Header (40 bytes)
	headerStart := 11
	hdr := data[headerStart:]

	if len(hdr) < 40 {
		return nil, fmt.Errorf("header 数据太短: %d", len(hdr))
	}

	// Characteristics: 16 bytes (skip for now)
	dataOffset := binary.LittleEndian.Uint64(hdr[16:24])
	dataSize := binary.LittleEndian.Uint64(hdr[24:32])
	indexOffset := binary.LittleEndian.Uint64(hdr[32:40])

	// IndexSize 需要从文件总大小推断，但我们现在只知道 offset
	// 实际实现中需要额外的 HEAD 请求获取 Content-Length
	// 这里先用 indexOffset 和 dataSize 的关系推断（通常 index 紧跟在 data 之后）
	// 但更好的做法是先 HEAD 拿到 Content-Length

	return &carV2Header{
		DataOffset:  dataOffset,
		DataSize:    dataSize,
		IndexOffset: indexOffset,
		// IndexSize 需要另外获取
	}, nil
}

// parseCarV2HeaderWithSize 使用已知的文件总大小解析 header
func (v *OnlineVerifier) parseCarV2HeaderWithSize(data []byte, fileSize int64) (*carV2Header, error) {
	hdr, err := v.parseCarV2Header(data)
	if err != nil {
		return nil, err
	}

	if hdr.IndexOffset > 0 {
		hdr.IndexSize = uint64(fileSize) - hdr.IndexOffset
	}

	return hdr, nil
}

// parseIndex 解析 CAR v2 Index 段
//
// Index 格式（CarIndexSorted / CarMultihashIndexSorted）:
//
//	第一个 varint: multicodec code
//	CarIndexSorted (0x0400):
//	  int32: bucket count
//	  每个 bucket:
//	    uint32: width (digest length + 8)
//	    uint64: data length
//	    []byte: compact index entries
//	     每项: digest (width-8 bytes) + offset (8 bytes LE)
//	CarMultihashIndexSorted (0x0401):
//	  int32: bucket count
//	  每个 bucket:
//	    uint64: multihash code
//	    然后是 multiWidthIndex 格式（同上 CarIndexSorted）
func (v *OnlineVerifier) parseIndex(data []byte) ([]IndexEntry, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("索引数据太短: %d bytes", len(data))
	}

	// 读取 multicodec code
	br := newBytesReader(data)
	codec, err := varint.ReadUvarint(br)
	if err != nil {
		return nil, fmt.Errorf("读取索引 codec 失败: %w", err)
	}

	pos := br.pos

	switch codec {
	case 0x0400: // CarIndexSorted
		return v.parseIndexSorted(data[pos:])
	case 0x0401: // CarMultihashIndexSorted
		return v.parseMultihashIndexSorted(data[pos:])
	default:
		return nil, fmt.Errorf("不支持的索引 codec: 0x%x", codec)
	}
}

// parseIndexSorted 解析 CarIndexSorted 格式的索引
func (v *OnlineVerifier) parseIndexSorted(data []byte) ([]IndexEntry, error) {
	pos := 0

	// 读取 bucket 数量
	if len(data) < 4 {
		return nil, fmt.Errorf("CarIndexSorted 数据太短")
	}
	bucketCount := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	if bucketCount < 0 || bucketCount > 100000 {
		return nil, fmt.Errorf("索引 bucket 数量异常: %d", bucketCount)
	}

	var allEntries []IndexEntry

	for i := 0; i < bucketCount; i++ {
		if pos+12 > len(data) {
			return nil, fmt.Errorf("bucket %d 数据越界", i)
		}

		width := binary.LittleEndian.Uint32(data[pos:])
		pos += 4
		dataLen := binary.LittleEndian.Uint64(data[pos:])
		pos += 8

		if width < 8 {
			return nil, fmt.Errorf("bucket %d width 太小: %d（至少需要 8 = digest+8offset）", i, width)
		}

		if pos+int(dataLen) > len(data) {
			return nil, fmt.Errorf("bucket %d 数据长度越界: pos=%d dataLen=%d len=%d", i, pos, dataLen, len(data))
		}

		digestLen := int(width) - 8
		entryCount := int(dataLen) / int(width)

		for j := 0; j < entryCount; j++ {
			entryStart := pos + j*int(width)
			digestEnd := entryStart + digestLen
			offsetEnd := digestEnd + 8

			digest := make([]byte, digestLen)
			copy(digest, data[entryStart:digestEnd])
			offset := binary.LittleEndian.Uint64(data[digestEnd:offsetEnd])

			allEntries = append(allEntries, IndexEntry{
				MultihashDigest: digest,
				Offset:          offset,
			})
		}

		pos += int(dataLen)
	}

	return allEntries, nil
}

// parseMultihashIndexSorted 解析 CarMultihashIndexSorted 格式的索引
func (v *OnlineVerifier) parseMultihashIndexSorted(data []byte) ([]IndexEntry, error) {
	pos := 0

	if len(data) < 4 {
		return nil, fmt.Errorf("MultihashIndexSorted 数据太短")
	}
	bucketCount := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	if bucketCount < 0 || bucketCount > 100000 {
		return nil, fmt.Errorf("索引 bucket 数量异常: %d", bucketCount)
	}

	var allEntries []IndexEntry

	for i := 0; i < bucketCount; i++ {
		if pos+8 > len(data) {
			return nil, fmt.Errorf("multihash bucket %d 数据越界", i)
		}

		// multihash code (8 bytes LE)
		mhCode := binary.LittleEndian.Uint64(data[pos:])
		pos += 8

		// 接下来是 multiWidthIndex 格式（int32 bucket count + buckets）
		entries, newPos, err := v.parseIndexSortedWithMHPos(data, pos, mhCode)
		if err != nil {
			return nil, fmt.Errorf("multihash bucket %d (code=%d) 解析失败: %w", i, mhCode, err)
		}
		pos = newPos
		allEntries = append(allEntries, entries...)
	}

	return allEntries, nil
}

// parseIndexSortedWithMHPos 解析 multiWidthIndex 中的索引条目（带 multihash code）
func (v *OnlineVerifier) parseIndexSortedWithMHPos(data []byte, startPos int, mhCode uint64) ([]IndexEntry, int, error) {
	pos := startPos

	if pos+4 > len(data) {
		return nil, pos, fmt.Errorf("multiWidthIndex 数据太短")
	}
	bucketCount := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	var allEntries []IndexEntry

	for i := 0; i < bucketCount; i++ {
		if pos+12 > len(data) {
			return nil, pos, fmt.Errorf("width bucket %d 越界", i)
		}

		width := binary.LittleEndian.Uint32(data[pos:])
		pos += 4
		dataLen := binary.LittleEndian.Uint64(data[pos:])
		pos += 8

		if width < 8 {
			return nil, pos, fmt.Errorf("width bucket %d width 太小: %d", i, width)
		}

		if pos+int(dataLen) > len(data) {
			return nil, pos, fmt.Errorf("width bucket %d 数据越界", i)
		}

		digestLen := int(width) - 8
		entryCount := int(dataLen) / int(width)

		for j := 0; j < entryCount; j++ {
			entryStart := pos + j*int(width)
			digestEnd := entryStart + digestLen
			offsetEnd := digestEnd + 8

			// 重建完整的 multihash
			digest := make([]byte, digestLen)
			copy(digest, data[entryStart:digestEnd])

			mh, err := multihash.Encode(digest, mhCode)
			if err != nil {
				continue // 跳过无法编码的条目
			}

			offset := binary.LittleEndian.Uint64(data[digestEnd:offsetEnd])

			allEntries = append(allEntries, IndexEntry{
				MultihashDigest: mh,
				Offset:          offset,
			})
		}

		pos += int(dataLen)
	}

	return allEntries, pos, nil
}

// randomSample 从索引条目中随机采样
func (v *OnlineVerifier) randomSample(entries []IndexEntry, count int) []IndexEntry {
	if count >= len(entries) {
		result := make([]IndexEntry, len(entries))
		copy(result, entries)
		return result
	}

	// Fisher-Yates 部分洗牌
	indices := rand.Perm(len(entries))
	result := make([]IndexEntry, count)
	for i := 0; i < count; i++ {
		result[i] = entries[indices[i]]
	}
	return result
}

// fetchRange 通过 HTTP Range 请求获取指定范围的字节
func (v *OnlineVerifier) fetchRange(txID string, offset, length int64) ([]byte, error) {
	return v.gateway.FetchRange(txID, offset, length)
}

// fetchFileSize 通过 HEAD 请求获取文件大小
func (v *OnlineVerifier) fetchFileSize(txID string) (int64, error) {
	headers, err := v.gateway.HeadTransaction(txID)
	if err != nil {
		return 0, err
	}
	contentLength := headers.Get("Content-Length")
	if contentLength == "" {
		return 0, fmt.Errorf("Content-Length header not found")
	}
	var size int64
	_, err = fmt.Sscanf(contentLength, "%d", &size)
	if err != nil {
		return 0, fmt.Errorf("invalid Content-Length: %s", contentLength)
	}
	return size, nil
}

// verifyBlock 验证单个 block：下载 → 计算 CID → 比对
func (v *OnlineVerifier) verifyBlock(dataTXID string, hdr *carV2Header, entry IndexEntry) (bool, error) {
	// block 在文件中的实际偏移量 = CAR v2 DataOffset + entry.Offset
	// entry.Offset 是在 CAR v1 data payload 内的偏移量
	fileOffset := int64(hdr.DataOffset) + int64(entry.Offset)

	// 先读取 block 头部（section length varint + CID）
	// 最少需要足够读取 varint 和 CID
	headData, err := v.fetchRange(dataTXID, fileOffset, 256) // 先读取前 256 字节
	if err != nil {
		return false, fmt.Errorf("读取 block @%d 失败: %w", fileOffset, err)
	}

	// 解析 section length
	br2 := newBytesReader(headData)
	sectionLen, err := varint.ReadUvarint(br2)
	if err != nil {
		return false, fmt.Errorf("解析 block section length @%d 失败: %w", fileOffset, err)
	}

	if sectionLen == 0 {
		return false, fmt.Errorf("block section length 为 0 @%d", fileOffset)
	}

	// CID 紧跟在 varint 之后
	cidStart := int64(br2.pos)
	cidEnd := cidStart + int64(sectionLen)

	// 如果 CID + 数据超出了我们读取的范围，需要再读取
	var blockData []byte
	if cidEnd+256 > int64(len(headData)) {
		// 需要更多数据
		totalNeeded := int64(br2.pos) + int64(sectionLen) + 256 // extra for safety
		blockData, err = v.fetchRange(dataTXID, fileOffset, totalNeeded)
		if err != nil {
			return false, fmt.Errorf("读取完整 block @%d 失败: %w", fileOffset, err)
		}
	} else {
		blockData = headData
	}

	// 解析 CID
	_, blockCID, err := cid.CidFromBytes(blockData[cidStart:cidEnd])
	if err != nil {
		return false, fmt.Errorf("解析 block CID @%d 失败: %w", fileOffset, err)
	}

	// 数据部分
	dataStart := cidStart + int64(blockCID.ByteLen())
	dataEnd := int64(br2.pos) + int64(sectionLen)
	data := blockData[dataStart:dataEnd]

	// 验证 CID：重新计算 multihash 并比对
	if err := ipfs.ValidateMultihash(blockCID.Hash()); err != nil {
		return false, fmt.Errorf("block @%d multihash 无效: %w", fileOffset, err)
	}

	// 计算数据的 hash 并比对
	hash, err := blockCID.Prefix().Sum(data)
	if err != nil {
		return false, fmt.Errorf("计算 block @%d hash 失败: %w", fileOffset, err)
	}

	if !hash.Equals(blockCID) {
		return false, fmt.Errorf("block @%d CID 不匹配: expected %s, got %s",
			fileOffset, blockCID.String(), hash.String())
	}

	log.Debug("在线验证：block @%d CID=%s ✅", fileOffset, blockCID.String())
	return true, nil
}

// ============================================================
// 工具类型
// ============================================================

// ipfsBytesReader 实现 io.ByteReader 接口，用于 varint 解析
type ipfsBytesReader struct {
	data []byte
	pos  int
}

func (r *ipfsBytesReader) ReadByte() (byte, error) {
	if r.pos >= len(r.data) {
		return 0, fmt.Errorf("EOF")
	}
	b := r.data[r.pos]
	r.pos++
	return b, nil
}

func (r *ipfsBytesReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.data) {
		return 0, fmt.Errorf("EOF")
	}
	n = copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

// newBytesReader 创建 bytesReader
func newBytesReader(data []byte) *ipfsBytesReader {
	return &ipfsBytesReader{data: data, pos: 0}
}

// ============================================================
// 与现有 pipeline 集成
// ============================================================

// PipelineVerifier 实现 pipeline 的 IndexVerifier 接口
// 将在线验证器包装为 pipeline 可用的验证函数
type PipelineVerifier struct {
	verifier *OnlineVerifier
	dataTXID string
}

// NewPipelineVerifier 创建 pipeline 用的验证器
func NewPipelineVerifier(verifier *OnlineVerifier, dataTXID string) *PipelineVerifier {
	return &PipelineVerifier{
		verifier: verifier,
		dataTXID: dataTXID,
	}
}

// VerifyIndex 验证 CAR v2 Index（实现 pipeline index verifier 接口）
func (pv *PipelineVerifier) VerifyIndex() error {
	result, err := pv.verifier.VerifyWithSampleCount(pv.dataTXID, 5)
	if err != nil {
		return fmt.Errorf("在线 Index 验证失败: %w", err)
	}
	if !result.Passed {
		return fmt.Errorf("在线 Index 验证未通过: verified=%d failed=%d errors=%v",
			result.VerifiedBlocks, result.FailedBlocks, result.Errors)
	}
	return nil
}

// GetGateway 获取网关客户端（用于共享网关连接）
func (v *OnlineVerifier) GetGateway() *download.Gateway {
	return v.gateway
}

// GetConfig 获取配置
func (v *OnlineVerifier) GetConfig() OnlineVerifierConfig {
	return v.config
}

// ForEachEntry 遍历索引中的所有条目（用于调试）
func (v *OnlineVerifier) ForEachEntry(dataTXID string, fn func(IndexEntry) bool) error {
	// 获取文件大小
	fileSize, err := v.fetchFileSize(dataTXID)
	if err != nil {
		return fmt.Errorf("获取文件大小失败: %w", err)
	}

	// 读取头部
	headerData, err := v.fetchRange(dataTXID, 0, 51)
	if err != nil {
		return err
	}

	version, err := v.readVersion(headerData)
	if err != nil || version != 2 {
		return fmt.Errorf("不是有效的 CAR v2 文件")
	}

	v2Header, err := v.parseCarV2Header(headerData)
	if err != nil {
		return err
	}

	// 计算 IndexSize
	v2Header.IndexSize = uint64(fileSize) - v2Header.IndexOffset

	if !v2Header.HasIndex() || v2Header.IndexSize == 0 {
		return fmt.Errorf("无索引")
	}

	// 读取索引
	indexData, err := v.fetchRange(dataTXID, int64(v2Header.IndexOffset), int64(v2Header.IndexSize))
	if err != nil {
		return err
	}

	entries, err := v.parseIndex(indexData)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if !fn(entry) {
			break
		}
	}
	return nil
}

// Must check we implement useful patterns
var _ = rand.Int
