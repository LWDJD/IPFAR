// Package discovery 提供 IPFAR 数据发现功能
//
// BlockWatcher 定期轮询 Arweave 最新区块高度，
// 发现新区块后检查是否包含 IPFAR 数据，
// 将新数据索引到 Store 并更新 Scanner 的 maxHeight。
package discovery

import (
	"context"
	"encoding/base64"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	sdkArweave "github.com/LWDJD/ipfar-sdk/arweave"

	"github.com/lwdjd/IPFAR/internal/index"
	"github.com/lwdjd/IPFAR/internal/log"
)

// BlockWatcher 监听新区块并检查 IPFAR 数据
type BlockWatcher struct {
	client   *sdkArweave.GatewayClient
	index    *index.Store
	verifier *LightVerifier

	// 最后检查的块高
	lastChecked uint64

	// 扫描器引用（用于同步 maxHeight）
	scanner *GraphQLScanner

	// 轮询间隔
	pollInterval time.Duration

	// 最新块高（原子操作）
	latestHeight uint64

	// 发现回调
	onBlockFound func(height uint64, metadataTXIDs []string)

	mu sync.Mutex

	// 生命周期
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// 统计
	blocksWatched uint64
	blocksFound   uint64
	startedAt     time.Time
}

// BlockWatcherConfig 区块监听器配置
type BlockWatcherConfig struct {
	// GatewayURL Arweave 网关 URL
	GatewayURL string
	// PollInterval 轮询间隔（默认 30s）
	PollInterval time.Duration
	// Timeout 请求超时（默认 15s）
	Timeout time.Duration
	// Index 索引存储
	Index *index.Store
	// Scanner 关联的扫描器
	Scanner *GraphQLScanner
}

// DefaultBlockWatcherConfig 返回默认配置
func DefaultBlockWatcherConfig() BlockWatcherConfig {
	return BlockWatcherConfig{
		GatewayURL:   "https://arweave.net",
		PollInterval: 30 * time.Second,
		Timeout:      15 * time.Second,
	}
}

// NewBlockWatcher 创建区块监听器
func NewBlockWatcher(cfg BlockWatcherConfig) *BlockWatcher {
	if cfg.GatewayURL == "" {
		cfg.GatewayURL = "https://arweave.net"
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 30 * time.Second
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 15 * time.Second
	}

	return &BlockWatcher{
		client:       sdkArweave.NewGatewayClient(cfg.GatewayURL),
		index:        cfg.Index,
		verifier:     NewLightVerifier(DefaultLightVerifierConfig()),
		scanner:      cfg.Scanner,
		pollInterval: cfg.PollInterval,
		startedAt:    time.Now(),
	}
}

// SetIndex 设置索引存储
func (w *BlockWatcher) SetIndex(store *index.Store) {
	w.index = store
}

// SetScanner 设置关联的扫描器
func (w *BlockWatcher) SetScanner(scanner *GraphQLScanner) {
	w.scanner = scanner
}

// GetLatestHeight 获取最新已知块高
func (w *BlockWatcher) GetLatestHeight() uint64 {
	return atomic.LoadUint64(&w.latestHeight)
}

// Start 开始监听新区块
func (w *BlockWatcher) Start(ctx context.Context) error {
	w.ctx, w.cancel = context.WithCancel(ctx)

	log.Info("区块监听器：启动，轮询间隔=%v", w.pollInterval)

	// 立即获取当前最新高度
	w.updateLatestHeight()

	w.wg.Add(1)
	go w.pollLoop()

	return nil
}

// Stop 停止监听
func (w *BlockWatcher) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
	w.wg.Wait()
	log.Info("区块监听器：已停止（观察到 %d 个区块，发现 %d 个含 IPFAR 数据）",
		w.blocksWatched, w.blocksFound)
}

// pollLoop 轮询循环
func (w *BlockWatcher) pollLoop() {
	defer w.wg.Done()

	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			w.pollOnce()
		}
	}
}

// pollOnce 执行一次轮询
func (w *BlockWatcher) pollOnce() {
	currentHeight := w.updateLatestHeight()
	log.Debug("区块监听器：轮询最新高度 当前=%d", currentHeight)
	last := atomic.LoadUint64(&w.lastChecked)

	if currentHeight <= last {
		return // 没有新区块
	}

	newBlocks := currentHeight - last
	log.Debug("区块监听器：发现新区块 height=%d（+%d 个新区块，%d → %d）", currentHeight, newBlocks, last+1, currentHeight)

	// 检查新区块中是否包含 IPFAR 数据
	fromHeight := last + 1
	if fromHeight <= 0 {
		fromHeight = currentHeight
	}

	// 批量查询新区块
	foundIDs, err := w.checkNewBlocks(fromHeight, currentHeight)
	if err != nil {
		log.Warn("区块监听器：检查新区块失败: %v", err)
		return
	}

	w.blocksWatched += newBlocks

	if len(foundIDs) > 0 {
		w.blocksFound++
		log.Debug("区块监听器：新块含 %d 个 IPFAR 交易 height=%d", len(foundIDs), currentHeight)
		log.Info("区块监听器：新区块 %d 包含 %d 个 IPFAR 元数据交易", currentHeight, len(foundIDs))

		if w.onBlockFound != nil {
			w.onBlockFound(currentHeight, foundIDs)
		}
	}

	// 更新 scanner 的 maxHeight
	if w.scanner != nil {
		w.scanner.SetMaxHeight(currentHeight)
	}

	atomic.StoreUint64(&w.lastChecked, currentHeight)
}

// checkNewBlocks 检查指定范围区块中是否包含 IPFAR 数据
// B4: 下载元数据 JSON，使用 LightVerifier.Verify() 验证，
// 解析完整参数写入 store.IndexMeta()，并调用 MarkVerified()
// M3: LightVerifier.Verify() 包含 PoW 验证（<100MiB 文件）
func (w *BlockWatcher) checkNewBlocks(fromHeight, toHeight uint64) ([]string, error) {
	ctx, cancel := context.WithTimeout(w.ctx, 15*time.Second)
	defer cancel()

	// 使用 GraphQL 查询
	protocolB64 := base64.RawURLEncoding.EncodeToString([]byte("IPFS-Arweave-Bridge"))
	ipfarTypeB64 := base64.RawURLEncoding.EncodeToString([]byte("meta"))

	q := sdkArweave.NewGraphQLQuery().
		AddTagFilter("Protocol", "IPFS-Arweave-Bridge", protocolB64).
		AddTagFilter("IPFAR-Type", "meta", ipfarTypeB64).
		SetBlockRange(int(fromHeight), int(toHeight)).
		SetFirst(100).
		SetSort("HEIGHT_ASC")

	txIDs, err := w.client.RunGraphQL(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("GraphQL 查询失败: %w", err)
	}

	// B4: 下载每个元数据 JSON，验证并索引完整参数
	validTxIDs := make([]string, 0, len(txIDs))
	for _, txID := range txIDs {
		if err := w.processBlockMeta(txID, fromHeight); err != nil {
			log.Warn("区块监听器：处理元数据 %s 失败: %v", txID, err)
			continue
		}
		validTxIDs = append(validTxIDs, txID)
	}

	return validTxIDs, nil
}

// processBlockMeta 处理单个区块中的元数据交易
// 1. 下载元数据 JSON
// 2. LightVerifier.Verify() 验证（含 PoW for <100MiB）
// 3. 解析完整参数写入 index.Store
// 4. 调用 MarkVerified() 标记验证状态
func (w *BlockWatcher) processBlockMeta(txID string, blockHeight uint64) error {
	// 检查是否已处理
	if w.index != nil {
		if verified, _ := w.index.IsVerified(txID); verified {
			log.Debug("区块监听器：元数据已通过 light 验证，跳过 %s", txID)
			return nil
		}
	}

	// 1. 下载元数据交易内容
	ctx, cancel := context.WithTimeout(w.ctx, 15*time.Second)
	defer cancel()

	metaJSON, err := w.client.DownloadTransactionData(ctx, txID)
	if err != nil {
		return fmt.Errorf("下载元数据交易失败: %w", err)
	}

	// 2. LightVerifier.Verify() 验证（含 PoW 验证 for <100MiB 文件 — M3）
	meta, err := w.verifier.Verify(metaJSON)
	if err != nil {
		log.Warn("区块监听器：轻量验证失败 txID=%s: %v", txID, err)
		// M3: 验证失败的不索引
		return fmt.Errorf("轻量验证失败: %w", err)
	}

	// 3. 构建完整参数并写入索引
	if w.index == nil {
		return nil
	}

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
		params["dataTxId"] = txID
	}
	if params["bundleTxId"] == "" {
		params["bundleTxId"] = "none"
	}
	// 如果 dataHeight 为 0（未从元数据中获取到），使用区块高度
	if params["blockHeight"] == "0" || params["blockHeight"] == "-1" {
		params["blockHeight"] = fmt.Sprintf("%d", blockHeight)
	}

	// 写入元数据索引
	if err := w.index.IndexMeta(txID, params); err != nil {
		return fmt.Errorf("索引元数据失败: %w", err)
	}

	// 索引 CID → metaTxID
	if meta.RootCID != "" {
		if err := w.index.IndexCID(meta.RootCID, txID); err != nil {
			log.Warn("区块监听器：索引 RootCID 失败 %s: %v", meta.RootCID, err)
		}
	}

	// 索引引用中的 CID
	if meta.HasReference() {
		refMap := *meta.Reference
		for refTXID, entry := range refMap {
			for _, cidStr := range entry.CIDs {
				if err := w.index.IndexCID(cidStr, refTXID); err != nil {
					log.Warn("区块监听器：索引引用 CID 失败 %s: %v", cidStr, err)
				}
			}
		}
	}

	// B4: 调用 MarkVerified() 标记验证状态
	if err := w.index.MarkVerified(txID); err != nil {
		log.Warn("区块监听器：标记已验证失败 %s: %v", txID, err)
	}

	log.Info("区块监听器：元数据已处理并标记验证 root_cid=%s meta_txid=%s height=%s size=%s",
		meta.RootCID, txID, params["blockHeight"], params["dataSize"])

	return nil
}

// updateLatestHeight 更新最新区块高度
// B3: 通过 GET /info 获取真实最新高度
func (w *BlockWatcher) updateLatestHeight() uint64 {
	ctx, cancel := context.WithTimeout(w.ctx, 15*time.Second)
	defer cancel()

	// 通过 SDK GatewayClient 的 /info 端点获取真实最新高度
	info, err := w.client.GetNetworkInfo(ctx)
	if err != nil {
		log.Warn("区块监听器：获取网络信息失败: %v，使用递增估算", err)
		// 回退：使用递增估算
		current := atomic.LoadUint64(&w.latestHeight)
		if current == 0 {
			current = 1919626 + 100000
		}
		current++
		atomic.StoreUint64(&w.latestHeight, current)
		return current
	}

	height := uint64(info.Height)
	if height > 0 {
		atomic.StoreUint64(&w.latestHeight, height)
		log.Debug("区块监听器：最新块高度 = %d（来自 /info）", height)
	}
	return height
}

// SetOnBlockFound 设置发现回调
func (w *BlockWatcher) SetOnBlockFound(fn func(height uint64, metadataTXIDs []string)) {
	w.onBlockFound = fn
}
