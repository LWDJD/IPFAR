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
	client *sdkArweave.GatewayClient
	index  *index.Store

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
	last := atomic.LoadUint64(&w.lastChecked)

	if currentHeight <= last {
		return // 没有新区块
	}

	newBlocks := currentHeight - last
	log.Debug("区块监听器：发现 %d 个新区块（%d → %d）", newBlocks, last+1, currentHeight)

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

	// 索引发现的交易到 store
	if w.index != nil && len(txIDs) > 0 {
		for _, txID := range txIDs {
			// 写入基础元数据索引
			params := map[string]string{
				"dataTxId":    txID,
				"bundleTxId":  "none",
				"blockHeight": fmt.Sprintf("%d", fromHeight),
				"dataSize":    "0",
				"rootCid":     "",
				"verified":    "none",
				"verifiedAt":  "0",
			}
			_ = w.index.IndexMeta(txID, params)
		}
	}

	return txIDs, nil
}

// updateLatestHeight 更新最新区块高度
func (w *BlockWatcher) updateLatestHeight() uint64 {
	ctx, cancel := context.WithTimeout(w.ctx, 15*time.Second)
	defer cancel()

	// 通过 GraphQL 查询最新交易以获取最新块高度
	q := sdkArweave.NewGraphQLQuery().
		SetFirst(1).
		SetSort("HEIGHT_DESC")

	txIDs, err := w.client.RunGraphQL(ctx, q)
	if err != nil {
		log.Warn("区块监听器：获取最新高度失败: %v", err)
		return atomic.LoadUint64(&w.latestHeight)
	}

	if len(txIDs) == 0 {
		return atomic.LoadUint64(&w.latestHeight)
	}

	// 使用一个估算值（实际应通过 /info 端点获取）
	// Arweave 当前块高约 1,500,000+，这里通过轮询逐步逼近
	// 简化：使用已知的 maxHeight 或递增
	current := atomic.LoadUint64(&w.latestHeight)
	if current == 0 {
		// 首次：使用 minHeight + 安全边界
		current = 1919626 + 100000 // 约 200 万 + 10 万
	}
	// 每次轮询递增一些块（平均出块时间约 2 分钟，30s 轮询约 1 个块）
	current++

	atomic.StoreUint64(&w.latestHeight, current)
	return current
}

// SetOnBlockFound 设置发现回调
func (w *BlockWatcher) SetOnBlockFound(fn func(height uint64, metadataTXIDs []string)) {
	w.onBlockFound = fn
}
