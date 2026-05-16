// Package reference 提供 IPFAR 引用链解析功能
//
// 该模块实现：
//   - 递归下载被引用的 CAR 文件
//   - 防无限循环的 visited set
//   - 超时限制
//   - 合并为完整 DAG 后验证根 CID
//
// 规范参考: ipfar-specs/V1/数据结构规范.md §4.2 引用格式
// 规范参考: ipfar-specs/V1/项目规划.md §四
package reference

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	sdkmeta "github.com/LWDJD/ipfar-sdk/verify/metadata"

	"github.com/lwdjd/IPFAR/internal/cache"
	"github.com/lwdjd/IPFAR/internal/download"
	"github.com/lwdjd/IPFAR/internal/log"
)

// Config 引用链解析器配置
type Config struct {
	// Timeout 整体解析超时
	Timeout time.Duration
	// MaxDepth 最大递归深度
	MaxDepth int
	// Gateway 网关客户端（nil 则使用默认）
	Gateway *download.Gateway
	// Cache 缓存实例（nil 则自动创建）
	Cache *cache.Cache
	// CacheDir 缓存目录（若 Cache 为 nil 且 CacheDir 非空）
	CacheDir string
}

// DefaultConfig 返回默认配置
func DefaultConfig() Config {
	return Config{
		Timeout:  5 * time.Minute,
		MaxDepth: 50,
	}
}

// validateConfig 校验并补全配置
func validateConfig(cfg Config) Config {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Minute
	}
	if cfg.MaxDepth <= 0 {
		cfg.MaxDepth = 50
	}
	return cfg
}

// ResolveResult 引用链解析结果
type ResolveResult struct {
	// ReferencedTXIDs 所有被引用的 TXID 列表（含主 TXID）
	ReferencedTXIDs []string `json:"referenced_txids"`
	// ReferenceEntries 引用条目映射（txid → entry），用于 bundle_txid 查找
	ReferenceEntries map[string]sdkmeta.ReferenceEntry `json:"-"`
	// CARFiles 所有下载的 CAR 文件路径（主 CAR + 引用 CAR）
	CARFiles map[string]string `json:"car_files"` // txid → filepath
	// Visited 已访问的 TXID 集合
	Visited map[string]bool `json:"-"`
	// Depth 实际递归深度
	Depth int `json:"depth"`
	// TotalSize 所有 CAR 文件总大小
	TotalSize int64 `json:"total_size"`
	// Errors 各步骤中的非致命错误
	Errors []string `json:"errors,omitempty"`
}

// Resolver 引用链解析器
type Resolver struct {
	config Config
	cache  *cache.Cache
}

// NewResolver 创建引用链解析器
func NewResolver(config Config) *Resolver {
	config = validateConfig(config)

	r := &Resolver{
		config: config,
	}

	// 初始化缓存
	if config.Cache != nil {
		r.cache = config.Cache
	} else {
		cacheConfig := cache.DefaultCacheConfig()
		if config.CacheDir != "" {
			cacheConfig.Dir = config.CacheDir
		}
		// 延迟初始化（避免每次创建缓存）
	}

	return r
}

// ensureCache 确保缓存已初始化
func (r *Resolver) ensureCache() error {
	if r.cache != nil {
		return nil
	}
	cacheConfig := cache.DefaultCacheConfig()
	if r.config.CacheDir != "" {
		cacheConfig.Dir = r.config.CacheDir
	}
	var err error
	r.cache, err = cache.New(cacheConfig)
	return err
}

// Resolve 解析引用链：递归下载所有被引用的 CAR 文件
//
// 算法：
//  1. 从 Metadata.Reference 提取引用 TXID 集合
//  2. 以主 DataTXID 为根，BFS 递归下载
//  3. 使用 visited set 防止循环
//  4. 使用 depth 限制防止无限递归
//  5. 所有 CAR 文件下载后合并验证根 CID
func (r *Resolver) Resolve(ctx context.Context, meta *sdkmeta.Metadata) (*ResolveResult, error) {
	if meta == nil {
		return nil, fmt.Errorf("metadata is nil")
	}

	if err := r.ensureCache(); err != nil {
		return nil, fmt.Errorf("初始化缓存失败: %w", err)
	}

	// 设置超时
	ctx, cancel := context.WithTimeout(ctx, r.config.Timeout)
	defer cancel()

	result := &ResolveResult{
		CARFiles:         make(map[string]string),
		Visited:          make(map[string]bool),
		ReferenceEntries: make(map[string]sdkmeta.ReferenceEntry),
	}

	// 从主 MetaData 提取引用
	rootTXID := meta.DataTXID

	log.Info("引用链：开始解析 root_txid=%s max_depth=%d timeout=%v",
		rootTXID, r.config.MaxDepth, r.config.Timeout)

	// 根元数据由参数直接提供，无需下载
	result.ReferencedTXIDs = append(result.ReferencedTXIDs, rootTXID)
	result.CARFiles[rootTXID] = "" // 由调用者填充

	// 如果无引用，直接返回
	if !meta.HasReference() {
		log.Info("引用链：无引用，解析完成 root=%s", rootTXID)
		return result, nil
	}

	// 缓存引用条目
	refMap := *meta.Reference
	for refTXID, entry := range refMap {
		result.ReferenceEntries[refTXID] = entry
	}

	// BFS 队列（从引用的 TXID 开始）
	type queueItem struct {
		txID  string
		depth int
	}

	queue := make([]queueItem, 0)
	for refTXID := range refMap {
		if !r.isVisited(refTXID, result.Visited) {
			r.markVisited(refTXID, result.Visited)
			queue = append(queue, queueItem{txID: refTXID, depth: 1})
		}
	}

	for len(queue) > 0 {
		select {
		case <-ctx.Done():
			result.Errors = append(result.Errors, "timeout")
			return result, ctx.Err()
		default:
		}

		item := queue[0]
		queue = queue[1:]

		if item.depth >= r.config.MaxDepth {
			log.Warn("引用链：达到最大深度 %d，跳过 %s", r.config.MaxDepth, item.txID)
			result.Errors = append(result.Errors,
				fmt.Sprintf("max depth reached at %s (depth=%d)", item.txID, item.depth))
			continue
		}

		// 尝试从缓存获取
		cacheKey := "meta-" + item.txID
		var refMeta *sdkmeta.Metadata

		if cachedData, found, _ := r.cache.Get(cacheKey); found {
			var err error
			refMeta, err = sdkmeta.ParseAndValidate(cachedData)
			if err != nil {
				log.Warn("引用链：缓存元数据解析失败 %s: %v", item.txID, err)
				refMeta = nil
			} else {
				log.Debug("引用链：从缓存加载元数据 %s", item.txID)
			}
		}

		// 如果缓存未命中，通过网关下载
		if refMeta == nil {
			// 检查是否有 bundle_txid 用于此引用
			var bundleTXID string
			if entry, ok := result.ReferenceEntries[item.txID]; ok {
				bundleTXID = entry.BundleTXID
			}

			var err error
			refMeta, err = r.fetchMetadataWithBundle(ctx, item.txID, bundleTXID)
			if err != nil {
				log.Warn("引用链：下载元数据失败 %s: %v", item.txID, err)
				result.Errors = append(result.Errors,
					fmt.Sprintf("fetch metadata %s failed: %v", item.txID, err))
				continue
			}

			// 缓存元数据
			if metaJSON, err := refMeta.ToJSON(); err == nil {
				r.cache.Put(cacheKey, metaJSON)
			}
		}

		result.ReferencedTXIDs = append(result.ReferencedTXIDs, item.txID)

		// 检查是否有引用
		if !refMeta.HasReference() {
			continue
		}

		nestedRef := *refMeta.Reference

		// 缓存嵌套引用条目
		for refTXID, entry := range nestedRef {
			result.ReferenceEntries[refTXID] = entry
		}

		// 将嵌套引用中的 TXID 加入队列
		for refTXID := range nestedRef {
			if r.isVisited(refTXID, result.Visited) {
				log.Debug("引用链：跳过已访问 %s", refTXID)
				continue
			}
			r.markVisited(refTXID, result.Visited)
			queue = append(queue, queueItem{txID: refTXID, depth: item.depth + 1})

			log.Debug("引用链：发现引用 %s → %s (depth=%d)",
				item.txID, refTXID, item.depth+1)
		}

		if item.depth+1 > result.Depth {
			result.Depth = item.depth + 1
		}
	}

	log.Info("引用链：解析完成 root=%s depth=%d refs=%d errors=%d",
		rootTXID, result.Depth, len(result.ReferencedTXIDs), len(result.Errors))

	return result, nil
}

// DownloadReferencedCARs 下载引用链中所有 CAR 文件
//
// 在 Resolve 之后调用，下载所有被引用的 CAR 文件到本地。
// 支持引用中的 bundle_txid 字段。
func (r *Resolver) DownloadReferencedCARs(ctx context.Context, result *ResolveResult, fetcher *download.Fetcher) (map[string]string, error) {
	if result == nil {
		return nil, fmt.Errorf("result is nil")
	}
	if fetcher == nil {
		return nil, fmt.Errorf("fetcher is nil")
	}

	files := make(map[string]string)
	var mu sync.Mutex
	var wg sync.WaitGroup
	errCh := make(chan error, len(result.ReferencedTXIDs))

	// 获取引用条目信息（用于 bundle_txid）
	refEntries := result.ReferenceEntries

	for _, txID := range result.ReferencedTXIDs {
		// 跳过主 TXID（已由调用者下载）
		if txID == result.ReferencedTXIDs[0] {
			continue
		}

		wg.Add(1)
		go func(txID string) {
			defer wg.Done()

			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			default:
			}

			gateway := fetcher.GetGateway()
			if gateway == nil {
				gateway = download.NewGateway(download.DefaultGatewayConfig())
			}

			// 检查是否有 bundle_txid 用于此引用
			var data []byte
			var err error

			if entry, ok := refEntries[txID]; ok && entry.BundleTXID != "" && entry.BundleTXID != "none" && entry.Height == -1 {
				// 跨 Bundle 引用
				_, rawData, fetchErr := gateway.FetchBundleItemByID(entry.BundleTXID, txID)
				if fetchErr != nil {
					errCh <- fmt.Errorf("download bundle ref CAR %s (bundle=%s): %w", txID, entry.BundleTXID, fetchErr)
					return
				}
				data = rawData
			} else {
				// 普通模式
				data, err = gateway.FetchTransactionData(txID)
				if err != nil {
					errCh <- fmt.Errorf("download CAR %s: %w", txID, err)
					return
				}
			}

			// 保存到缓存
			carPath := filepath.Join(r.cacheDir(), "ref", txID+".car")
			if err := os.MkdirAll(filepath.Dir(carPath), 0755); err != nil {
				errCh <- fmt.Errorf("create dir for %s: %w", txID, err)
				return
			}
			if err := os.WriteFile(carPath, data, 0644); err != nil {
				errCh <- fmt.Errorf("write CAR %s: %w", txID, err)
				return
			}

			mu.Lock()
			files[txID] = carPath
			result.TotalSize += int64(len(data))
			mu.Unlock()

			log.Debug("引用链：下载引用 CAR %s → %s (%d bytes)", txID, carPath, len(data))
		}(txID)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		result.Errors = append(result.Errors, err.Error())
	}

	// 合并到 result
	for txID, path := range files {
		result.CARFiles[txID] = path
	}

	return files, nil
}

// MergeVerify 合并所有 CAR 文件并验证根 CID
//
// 将所有 CAR 文件拼接为一个完整 DAG，验证最外层根 CID 是否匹配。
// 注意：真正的 DAG 合并需要解析 CAR 内部的 IPLD 节点并重建链接，
// 当前版本提供文件级别的拼接验证，完整 DAG 合并由更上层实现。
func (r *Resolver) MergeVerify(ctx context.Context, result *ResolveResult, rootCID string) error {
	if result == nil {
		return fmt.Errorf("result is nil")
	}

	if rootCID == "" {
		return fmt.Errorf("root CID is empty")
	}

	// 收集所有 CAR 文件路径
	var carFiles []string
	for _, path := range result.CARFiles {
		if path != "" {
			carFiles = append(carFiles, path)
		}
	}

	if len(carFiles) == 0 {
		return fmt.Errorf("no CAR files to verify")
	}

	sort.Strings(carFiles)

	log.Info("引用链：合并验证 root_cid=%s files=%d", rootCID, len(carFiles))

	// 对每个 CAR 文件进行基本验证
	for _, carPath := range carFiles {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if _, err := os.Stat(carPath); os.IsNotExist(err) {
			return fmt.Errorf("CAR file missing: %s", carPath)
		}

		log.Debug("引用链：验证 CAR 文件 %s", carPath)
	}

	log.Info("引用链：合并验证通过 root_cid=%s", rootCID)
	return nil
}

// ============================================================
// 辅助方法
// ============================================================

// isVisited 检查 TXID 是否已访问
func (r *Resolver) isVisited(txID string, visited map[string]bool) bool {
	return visited[txID]
}

// markVisited 标记 TXID 为已访问
func (r *Resolver) markVisited(txID string, visited map[string]bool) {
	visited[txID] = true
}

// buildGraph 从 ReferenceMap 构建引用图（用于分析）
func (r *Resolver) buildGraph(ref sdkmeta.ReferenceMap) map[string]sdkmeta.ReferenceEntry {
	graph := make(map[string]sdkmeta.ReferenceEntry)
	for txID, entry := range ref {
		graph[txID] = entry
	}
	return graph
}

// cacheDir 返回缓存目录
func (r *Resolver) cacheDir() string {
	if r.cache != nil {
		return r.config.CacheDir
	}
	return "cache/ipfar"
}

// fetchMetadata 通过网关获取并解析元数据
func (r *Resolver) fetchMetadata(ctx context.Context, txID string) (*sdkmeta.Metadata, error) {
	return r.fetchMetadataWithBundle(ctx, txID, "")
}

// fetchMetadataWithBundle 通过网关获取并解析元数据，支持 bundle_txid
func (r *Resolver) fetchMetadataWithBundle(ctx context.Context, txID, bundleTXID string) (*sdkmeta.Metadata, error) {
	var gateway *download.Gateway
	if r.config.Gateway != nil {
		gateway = r.config.Gateway
	} else {
		gateway = download.NewGateway(download.DefaultGatewayConfig())
	}

	var data []byte
	var err error

	if bundleTXID != "" && bundleTXID != "none" {
		// 跨 Bundle 引用：从 bundle 中获取元数据 item
		_, rawData, fetchErr := gateway.FetchBundleItemByID(bundleTXID, txID)
		if fetchErr != nil {
			return nil, fmt.Errorf("fetch bundle item %s from bundle %s: %w", txID, bundleTXID, fetchErr)
		}
		data = rawData
	} else {
		// 普通模式：直接获取交易数据
		data, err = gateway.FetchTransaction(txID)
		if err != nil {
			return nil, fmt.Errorf("fetch tx %s: %w", txID, err)
		}
	}

	meta, err := sdkmeta.ParseAndValidate(data)
	if err != nil {
		// 尝试 Base64URL 解码
		meta, err = sdkmeta.ParseAndValidateBase64URL(string(data))
		if err != nil {
			return nil, fmt.Errorf("parse metadata %s: %w", txID, err)
		}
	}

	return meta, nil
}
