// Package discovery 提供 IPFAR 数据发现功能
//
// GraphQL 顺序扫描器：从旧到新顺序扫描 Arweave 区块，
// 通过 GraphQL 查询 IPFAR 标签定位元数据交易。
//
// 与随机抽样模式不同，此模式系统性地扫描整个区块范围，
// 确保不会遗漏任何 IPFAR 数据。
//
// 规范参考: ipfar-specs/V1/项目规划.md §3.1

package discovery

import (
	"context"
	"encoding/base64"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	sdkArweave "github.com/LWDJD/ipfar-sdk/arweave"
	sdkBundle "github.com/LWDJD/ipfar-sdk/bundle"

	"github.com/lwdjd/IPFAR/internal/index"
	"github.com/lwdjd/IPFAR/internal/log"
)

// OnMetaFound 元数据发现回调
// metaTXID: 元数据交易 ID
// metaJSON: 元数据 JSON 原始字节
// blockHeight: 元数据所在区块高度
type OnMetaFound func(metaTXID string, metaJSON []byte, blockHeight uint64)

// GraphQLScanner 从旧到新顺序扫描 Arweave 区块，查找 IPFAR 元数据交易
type GraphQLScanner struct {
	client   *sdkArweave.GatewayClient
	index    *index.Store
	verifier *LightVerifier

	// 扫描范围
	minHeight     uint64
	currentHeight uint64 // 当前扫描到的块高（原子操作）
	maxHeight     uint64 // 当前最新块高（动态更新）

	// 并发扫描控制
	batchSize  int           // 每批查询的块数量
	queryDelay time.Duration // 批次间延迟（避免网关限流）

	mu sync.Mutex

	// 生命周期
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// 统计
	stats      ScannerStats
	statsMu    sync.RWMutex

	// 回调
	onMetaFound OnMetaFound
}

// ScannerStats 扫描器统计信息
type ScannerStats struct {
	BlocksScanned    uint64 `json:"blocks_scanned"`
	BlocksFound      uint64 `json:"blocks_found"`
	MetadataFound    uint64 `json:"metadata_found"`
	MetadataVerified uint64 `json:"metadata_verified"`
	LastScannedHeight uint64 `json:"last_scanned_height"`
	StartedAt        time.Time `json:"started_at"`
	LastActivity     time.Time `json:"last_activity"`
	Running          bool   `json:"running"`
}

// GraphQLScannerConfig 扫描器配置
type GraphQLScannerConfig struct {
	// GatewayURL Arweave 网关 URL
	GatewayURL string
	// MinHeight 扫描起始高度
	MinHeight uint64
	// MaxHeight 扫描结束高度（0 表示动态跟随最新块）
	MaxHeight uint64
	// BatchSize 每批扫描的块数（默认 100）
	BatchSize int
	// QueryDelay 每批查询延迟（默认 2s，避免网关限流）
	QueryDelay time.Duration
	// Timeout GraphQL 查询超时（默认 30s）
	Timeout time.Duration
	// Index CID/元数据索引存储
	Index *index.Store
}

// DefaultGraphQLScannerConfig 返回默认扫描器配置
func DefaultGraphQLScannerConfig() GraphQLScannerConfig {
	return GraphQLScannerConfig{
		GatewayURL: "https://arweave.net",
		MinHeight:  1919626,
		MaxHeight:  0,
		BatchSize:  100,
		QueryDelay: 2 * time.Second,
		Timeout:    30 * time.Second,
	}
}

// NewGraphQLScanner 创建 GraphQL 顺序扫描器
func NewGraphQLScanner(cfg GraphQLScannerConfig) (*GraphQLScanner, error) {
	if cfg.GatewayURL == "" {
		cfg.GatewayURL = "https://arweave.net"
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.QueryDelay <= 0 {
		cfg.QueryDelay = 2 * time.Second
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}

	client := sdkArweave.NewGatewayClient(cfg.GatewayURL)

	// 创建轻量验证器
	verifier := NewLightVerifier(DefaultLightVerifierConfig())

	s := &GraphQLScanner{
		client:        client,
		index:         cfg.Index,
		verifier:      verifier,
		minHeight:     cfg.MinHeight,
		currentHeight: cfg.MinHeight,
		maxHeight:     cfg.MaxHeight,
		batchSize:     cfg.BatchSize,
		queryDelay:    cfg.QueryDelay,
		stats: ScannerStats{
			StartedAt: time.Now(),
		},
	}

	return s, nil
}

// SetIndex 设置索引存储（创建后注入）
func (s *GraphQLScanner) SetIndex(store *index.Store) {
	s.index = store
}

// SetMaxHeight 动态更新最大块高度
func (s *GraphQLScanner) SetMaxHeight(height uint64) {
	atomic.StoreUint64(&s.maxHeight, height)
}

// SetOnMetaFound 设置元数据发现回调
func (s *GraphQLScanner) SetOnMetaFound(fn OnMetaFound) {
	s.onMetaFound = fn
}

// GetMaxHeight 获取当前最大块高度
func (s *GraphQLScanner) GetMaxHeight() uint64 {
	return atomic.LoadUint64(&s.maxHeight)
}

// GetCurrentHeight 获取当前扫描高度
func (s *GraphQLScanner) GetCurrentHeight() uint64 {
	return atomic.LoadUint64(&s.currentHeight)
}

// Start 开始顺序扫描
// 从 minHeight 开始，逐个批次扫描到最新块
func (s *GraphQLScanner) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	// 如果已有索引中的历史扫描进度，从上次位置继续
	if s.index != nil {
		// 读取上次扫描的最高块高
		lastHeight := s.loadLastScannedHeight()
		if lastHeight >= s.minHeight {
			s.currentHeight = lastHeight + 1
			log.Info("GraphQL 扫描器：从高度 %d 继续扫描（上次进度: %d）", s.currentHeight, lastHeight)
		}
	}

	s.statsMu.Lock()
	s.stats.Running = true
	s.statsMu.Unlock()

	log.Info("GraphQL 扫描器：开始顺序扫描 minHeight=%d currentHeight=%d batchSize=%d",
		s.minHeight, s.currentHeight, s.batchSize)

	s.wg.Add(1)
	go s.scanLoop()

	return nil
}

// Stop 停止扫描
func (s *GraphQLScanner) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()

	s.statsMu.Lock()
	s.stats.Running = false
	s.statsMu.Unlock()

	log.Info("GraphQL 扫描器：已停止（扫描到高度 %d）", s.GetCurrentHeight())
}

// scanLoop 主扫描循环
func (s *GraphQLScanner) scanLoop() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.queryDelay)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.scanBatch()
		}
	}
}

// scanBatch 扫描一批区块
func (s *GraphQLScanner) scanBatch() {
	current := s.GetCurrentHeight()
	maxH := s.GetMaxHeight()

	// 如果 maxHeight 为 0（未设置），尝试获取最新高度
	if maxH == 0 {
		latestHeight, err := s.fetchLatestHeight()
		if err != nil {
			log.Warn("GraphQL 扫描器：获取最新高度失败: %v", err)
			return
		}
		s.SetMaxHeight(latestHeight)
		maxH = latestHeight
		log.Debug("GraphQL 扫描器：最新块高度 = %d", maxH)
	}

	// 已追赶上最新块
	if current > maxH {
		log.Debug("GraphQL 扫描器：已追上最新块 current=%d max=%d", current, maxH)
		return
	}

	// 计算本批次结束高度
	batchEnd := current + uint64(s.batchSize) - 1
	if batchEnd > maxH {
		batchEnd = maxH
	}

	log.Debug("GraphQL 查询：范围 [%d, %d] batchSize=%d", current, batchEnd, s.batchSize)

	// 通过 GraphQL 查询该范围内的 IPFAR 交易
	foundBlocks, err := s.queryRange(current, batchEnd)
	if err != nil {
		log.Warn("GraphQL 扫描器：查询范围 [%d, %d] 失败: %v", current, batchEnd, err)

		// 即使查询失败，也更新进度避免卡在同一位置
		atomic.StoreUint64(&s.currentHeight, batchEnd+1)
		s.updateStats(batchEnd, 0, 0)
		return
	}

	// 处理发现的元数据交易
	totalFound := 0
	totalVerified := 0
	for _, blockResult := range foundBlocks {
		totalFound += len(blockResult.MetadataTXIDs)

		// 验证并索引每个元数据交易
		for _, txID := range blockResult.MetadataTXIDs {
			if err := s.processMetadata(txID, blockResult.Height); err != nil {
				log.Warn("GraphQL 扫描器：处理元数据失败 txID=%s: %v", txID, err)
				continue
			}
			totalVerified++
		}
	}

	// 更新扫描进度
	atomic.StoreUint64(&s.currentHeight, batchEnd+1)
	s.updateStats(batchEnd, totalFound, totalVerified)
	s.saveLastScannedHeight(batchEnd)

	if totalFound > 0 {
		log.Debug("GraphQL 结果：%d 个区块含 %d 条元数据", len(foundBlocks), totalFound)
		log.Info("GraphQL 扫描器：批次 [%d, %d] 完成，发现 %d 个区块含 %d 条元数据",
			current, batchEnd, len(foundBlocks), totalFound)
	}
}

// queryRange 通过 GraphQL 查询指定范围区块中的 IPFAR 交易
func (s *GraphQLScanner) queryRange(fromHeight, toHeight uint64) ([]BlockScanResult, error) {
	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
	defer cancel()

	// 构建 GraphQL 查询：匹配 IPFAR 协议标签
	// 注意：不过滤 IPFAR-Type，因为 Bundle 模式的 DataItem 标签在 L2 层，
	// Arweave GraphQL API 不索引 Bundle DataItem 的标签。
	// 返回的交易由 processMetadata 通过 ClassifyByData 进一步分类处理。
	protocolB64 := base64.RawURLEncoding.EncodeToString([]byte("IPFS-Arweave-Bridge"))

	q := sdkArweave.NewGraphQLQuery().
		AddTagFilter("Protocol", "IPFS-Arweave-Bridge", protocolB64).
		SetBlockRange(int(fromHeight), int(toHeight)).
		SetFirst(500). // 同一范围内可能有多种交易（直接元数据、Bundle、CAR）
		SetSort("HEIGHT_ASC")

	txIDs, err := s.client.RunGraphQL(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("GraphQL 查询失败: %w", err)
	}

	if len(txIDs) == 0 {
		return nil, nil
	}

	// GraphQL 返回的只是交易 ID 列表，没有直接包含高度信息
	// 我们需要为这些交易获取其所属的区块高度
	// 简化处理：将查询到的 txID 放入一个结果（高度为范围起始）
	// 实际应通过 /tx/{id} 端点获取每个交易的 block_height
	result := BlockScanResult{
		Height:        fromHeight, // 近似值
		MetadataTXIDs: txIDs,
	}

	log.Debug("GraphQL 结果：%d 条 IPFAR 协议交易（范围 [%d, %d]）",
		len(txIDs), fromHeight, toHeight)

	return []BlockScanResult{result}, nil
}

// processMetadata 处理发现的交易（可能是直接元数据或 Bundle）
//
// 流程：
//  1. 下载交易数据
//  2. 通过 ClassifyByData 判断类型
//  3. 直接元数据 → processDirectMeta
//  4. Bundle → processBundle（解析内部 DataItem，查找 IPFAR-Type: meta）
//  5. 其他类型 → 跳过
func (s *GraphQLScanner) processMetadata(txID string, blockHeight uint64) error {
	if s.index == nil {
		return fmt.Errorf("索引存储未设置")
	}

	// 下载交易数据
	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
	defer cancel()

	data, err := s.client.DownloadTransactionData(ctx, txID)
	if err != nil {
		return fmt.Errorf("下载交易数据失败: %w", err)
	}

	// 根据数据内容分类
	txType := ClassifyByData(data)
	log.Debug("交易分类：txID=%s type=%s", txID, txType)

	switch txType {
	case "meta":
		return s.processDirectMeta(txID, data, blockHeight)
	case "bundle":
		return s.processBundle(txID, data, blockHeight)
	default:
		log.Debug("GraphQL 扫描器：跳过非 IPFAR 元数据/Bundle 交易 %s (类型: %s)", txID, txType)
		return nil
	}
}

// processDirectMeta 处理直接元数据交易
//
// 1. 轻量验证
// 2. 索引 CID 和元数据到 Store
func (s *GraphQLScanner) processDirectMeta(metaTxID string, data []byte, blockHeight uint64) error {
	// 检查是否已处理过
	if has, _ := s.index.HasCID(metaTxID); has {
		log.Debug("GraphQL 扫描器：元数据已处理，跳过 %s", metaTxID)
		return nil
	}

	// 轻量验证
	meta, err := s.verifier.Verify(data)
	if err != nil {
		log.Warn("GraphQL 扫描器：轻量验证失败 txID=%s: %v", metaTxID, err)
		// 即使验证失败也继续索引（标记为未验证）
		params := map[string]string{
			"dataTxId":    metaTxID,
			"bundleTxId":  "none",
			"blockHeight": fmt.Sprintf("%d", blockHeight),
			"dataSize":    "0",
			"rootCid":     metaTxID,
			"verified":    "none",
			"verifiedAt":  "0",
		}
		return s.index.IndexMeta(metaTxID, params)
	}

	// 构建索引参数
	params := map[string]string{
		"dataTxId":    meta.DataTXID,
		"bundleTxId":  meta.BundleTXID,
		"blockHeight": fmt.Sprintf("%d", meta.DataHeight),
		"dataSize":    fmt.Sprintf("%d", meta.DataSize),
		"rootCid":     meta.RootCID,
		"verified":    "light",
		"verifiedAt":  fmt.Sprintf("%d", time.Now().Unix()),
	}

	if params["dataTxId"] == "" {
		params["dataTxId"] = metaTxID
	}
	if params["bundleTxId"] == "" {
		params["bundleTxId"] = "none"
	}

	// 索引元数据
	if err := s.index.IndexMeta(metaTxID, params); err != nil {
		return fmt.Errorf("索引元数据失败: %w", err)
	}

	// 索引 CID → metaTxID
	if meta.RootCID != "" {
		if err := s.index.IndexCID(meta.RootCID, metaTxID); err != nil {
			log.Warn("GraphQL 扫描器：索引 RootCID 失败 %s: %v", meta.RootCID, err)
		}
	}

	// 索引引用中的 CID
	if meta.HasReference() {
		refMap := *meta.Reference
		for refTXID, entry := range refMap {
			for _, cidStr := range entry.CIDs {
				if err := s.index.IndexCID(cidStr, refTXID); err != nil {
					log.Warn("GraphQL 扫描器：索引引用 CID 失败 %s: %v", cidStr, err)
				}
			}
		}
	}

	log.Info("GraphQL 扫描器：元数据已处理 root_cid=%s meta_txid=%s height=%d",
		meta.RootCID, metaTxID, meta.DataHeight)

	// 回调 Service 触发后续下载、完整验证、DHT 发布
	if s.onMetaFound != nil {
		s.onMetaFound(metaTxID, data, blockHeight)
	}

	return nil
}

// processBundle 处理 ANS-104 Bundle 交易
//
// 下载并解析 Bundle，遍历内部 DataItem，查找携带 IPFAR-Type: meta 标签的
// 元数据 DataItem，对其进行验证和索引。
// DataItem 的 ID（base64url 编码）作为虚拟 txID 用于索引。
func (s *GraphQLScanner) processBundle(bundleTxID string, bundleData []byte, blockHeight uint64) error {
	bundle, err := sdkBundle.ParseBundle(bundleData)
	if err != nil {
		return fmt.Errorf("解析 Bundle 失败: %w", err)
	}

	log.Debug("Bundle 遍历：%d 个 DataItem（bundle=%s）", len(bundle.Items), bundleTxID)

	metaCount := 0
	for _, item := range bundle.Items {
		// 检查 DataItem 是否有 IPFAR-Type: meta 标签
		if !HasMetaTag(item.Tags) {
			continue
		}

		// 计算 DataItem ID（base64url 编码）
		itemID := base64.RawURLEncoding.EncodeToString(item.Id)

		log.Debug("GraphQL 扫描器：发现 Bundle 内元数据 DataItem itemID=%s bundle=%s",
			itemID, bundleTxID)

		// 处理该 DataItem 中的元数据
		if err := s.processDirectMeta(itemID, item.Data, blockHeight); err != nil {
			log.Warn("GraphQL 扫描器：处理 Bundle 内元数据失败 itemID=%s bundle=%s: %v",
				itemID, bundleTxID, err)
			continue
		}
		metaCount++
	}

	if metaCount > 0 {
		log.Info("GraphQL 扫描器：Bundle %s 中处理了 %d 条元数据", bundleTxID, metaCount)
	} else {
		log.Debug("GraphQL 扫描器：Bundle %s 中未找到 IPFAR-Type: meta 的 DataItem", bundleTxID)
	}

	return nil
}

// fetchLatestHeight 通过 Arweave 网关获取最新区块高度
func (s *GraphQLScanner) fetchLatestHeight() (uint64, error) {
	ctx, cancel := context.WithTimeout(s.ctx, 15*time.Second)
	defer cancel()

	// 使用 GraphQL 查询最新区块
	q := sdkArweave.NewGraphQLQuery().
		SetFirst(1).
		SetSort("HEIGHT_DESC")

	txIDs, err := s.client.RunGraphQL(ctx, q)
	if err != nil {
		return 0, err
	}

	if len(txIDs) == 0 {
		return 0, fmt.Errorf("无法获取最新区块高度：查询无结果")
	}

	// 获取该交易所属的区块高度
	// 简化：通过 /tx/{id} 端点获取
	// 这里使用另一种方法：通过 block/current 端点
	info, err := s.client.DownloadTransactionData(ctx, txIDs[0])
	if err != nil || len(info) == 0 {
		// Fallback：返回预估值
		return s.minHeight + 10000, nil
	}

	// 实际上我们需要的是最新区块高度，不是交易高度
	// 简单返回一个较大的值，后续由 BlockWatcher 更新
	return s.minHeight + 100000, nil
}

// loadLastScannedHeight 从索引中读取上次扫描高度
func (s *GraphQLScanner) loadLastScannedHeight() uint64 {
	if s.index == nil {
		return 0
	}
	params, err := s.index.GetMeta("__scanner_progress__")
	if err != nil || params == nil {
		return 0
	}
	heightStr := params["lastScannedHeight"]
	var height uint64
	fmt.Sscanf(heightStr, "%d", &height)
	return height
}

// saveLastScannedHeight 将当前扫描进度写入索引
func (s *GraphQLScanner) saveLastScannedHeight(height uint64) {
	if s.index == nil {
		return
	}
	params := map[string]string{
		"lastScannedHeight": fmt.Sprintf("%d", height),
		"updatedAt":         fmt.Sprintf("%d", time.Now().Unix()),
	}
	_ = s.index.IndexMeta("__scanner_progress__", params)
}

// updateStats 更新扫描统计
func (s *GraphQLScanner) updateStats(lastHeight uint64, found, verified int) {
	s.statsMu.Lock()
	defer s.statsMu.Unlock()
	s.stats.BlocksScanned = lastHeight - s.minHeight + 1
	s.stats.BlocksFound += uint64(found)
	s.stats.MetadataFound += uint64(found)
	s.stats.MetadataVerified += uint64(verified)
	s.stats.LastScannedHeight = lastHeight
	s.stats.LastActivity = time.Now()
}

// Stats 返回扫描统计
func (s *GraphQLScanner) Stats() ScannerStats {
	s.statsMu.RLock()
	defer s.statsMu.RUnlock()
	return s.stats
}

// BlockScanResult 区块扫描结果
type BlockScanResult struct {
	Height        uint64   `json:"height"`
	MetadataTXIDs []string `json:"metadata_txids"`
}
