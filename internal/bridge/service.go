// Package bridge 提供 IPFAR 桥接核心逻辑
package bridge

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	sdkmeta "github.com/LWDJD/ipfar-sdk/verify/metadata"
	"github.com/LWDJD/ipfar-sdk/verify/pipeline"
	"github.com/dgraph-io/badger/v4"
	"github.com/ipfs/go-cid"

	"github.com/lwdjd/IPFAR/internal/bitswap"
	"github.com/lwdjd/IPFAR/internal/cache"
	"github.com/lwdjd/IPFAR/internal/discovery"
	"github.com/lwdjd/IPFAR/internal/dht"
	"github.com/lwdjd/IPFAR/internal/download"
	"github.com/lwdjd/IPFAR/internal/index"
	"github.com/lwdjd/IPFAR/internal/log"
	"github.com/lwdjd/IPFAR/internal/store"
	"github.com/lwdjd/IPFAR/internal/verify"
)

// ServiceConfig 桥接服务配置
type ServiceConfig struct {
	// Preset 安全预设: strict, balanced, light, trusted
	Preset string
	// VerifyPoW 独立 PoW 开关（覆盖 preset）
	VerifyPoW bool
	// VerifyIndex 独立 Index 开关（覆盖 preset）
	VerifyIndex bool
	// VerifyReferenceChain 独立引用链开关（覆盖 preset）
	VerifyReferenceChain bool
	// VerifyIntegrity 独立完整性开关（覆盖 preset）
	VerifyIntegrity bool

	// Discovery 发现配置
	DiscoveryMode  string // "sampling"（默认）或 "graphql"
	MinBlockHeight uint64
	MaxBlockHeight uint64
	PollInterval   time.Duration

	// Download 下载配置
	GatewayURLs []string
	CacheDir    string
	MaxFileSize int64

	// Pipeline 管道配置
	CarAvailable bool // 是否在验证前下载 CAR 到本地缓存（默认 true）

	// 在线验证配置（路径 A：不存盘，Range 采样验证）
	OnlineVerify          bool // 是否启用在线验证（默认 false）
	OnlineSampleCount     int  // 在线验证采样 block 数量（默认 5）
	OnlineMaxConcurrency  int  // 在线验证最大并发数（默认 4）
	DownloadMaxConcurrency int  // 完整下载最大并发数（默认 2）

	// DHT 内容发布配置（规范 P3-1）
	DHTEnabled          bool     // 是否启用 DHT 内容发布
	DHTMode             string   // DHT 模式: "server" / "client"
	DHTBootstrapPeers   []string // DHT 引导节点
	DHTReprovideInterval string   // 重新提供间隔
	DHTProvideConcurrency int    // 提供并发数
	DHTListenAddresses  []string // libp2p 监听地址

	// Bitswap 按需拉取配置
	BitswapEnabled bool   // 是否启用 Bitswap 服务
	BitswapPort    int    // Bitswap 监听端口（默认 4001）
	CacheSize      int64  // 缓存最大字节数（默认 1GB，用于 Bitswap 块缓存）
}

// DefaultServiceConfig 返回默认服务配置
func DefaultServiceConfig() ServiceConfig {
	return ServiceConfig{
		Preset:                 pipeline.SecurityLight,
		DiscoveryMode:          "sampling",
		MinBlockHeight:         0,
		MaxBlockHeight:         0, // 动态跟随
		PollInterval:           2 * time.Minute,
		CacheDir:               "cache/car",
		MaxFileSize:            0, // 不限制
		CarAvailable:           true,
		OnlineVerify:           false,
		OnlineSampleCount:      5,
		OnlineMaxConcurrency:   4,
		DownloadMaxConcurrency: 2,
	}
}

// Service 桥接服务，集成发现 → 下载 → 验证完整链路
type Service struct {
	config  ServiceConfig
	bridge  *Bridge
	fetcher *download.Fetcher
	sampler *discovery.Sampler

	// DHT 内容发布提供器
	dhtProvider *dht.Provider

	// 在线验证器（延迟初始化）
	onlineVerifier     *verify.OnlineVerifier
	onlineVerifierOnce sync.Once

	// Bitswap 按需拉取架构
	bitswapService *bitswap.Service // Bitswap 服务端
	indexStore     *index.Store     // CID → Arweave 索引
	blockFetcher   *BlockFetcher    // 按需拉取器
	badgerDB       *badger.DB       // Badger 实例（复用）
	cacheStore     *store.Store     // KV 存储（复用）

	// 并发控制信号量
	downloadSema chan struct{} // 完整下载并发控制
	onlineSema   chan struct{} // 在线验证并发控制

	// 上下文
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// 统计
	mu    sync.RWMutex
	stats ServiceStats
}

// ServiceStats 服务统计
type ServiceStats struct {
	BlocksChecked     uint64    `json:"blocks_checked"`
	BlocksFound       uint64    `json:"blocks_found"`
	MetadataFetched   uint64    `json:"metadata_fetched"`
	CARsDownloaded    uint64    `json:"cars_downloaded"`
	OnlineVerified    uint64    `json:"online_verified"`
	PipelinesPassed   uint64    `json:"pipelines_passed"`
	PipelinesFailed   uint64    `json:"pipelines_failed"`
	StartedAt         time.Time `json:"started_at"`
	LastActivity      time.Time `json:"last_activity"`
}

// NewService 创建新的桥接服务
func NewService(cfg ServiceConfig) (*Service, error) {
	// 解析安全预设
	verifyPoW := cfg.VerifyPoW
	verifyIndex := cfg.VerifyIndex
	verifyRef := cfg.VerifyReferenceChain
	verifyIntegrity := cfg.VerifyIntegrity

	// 如果设置了 preset，优先使用 preset
	if cfg.Preset != "" {
		presetConfig, err := pipeline.GetPreset(cfg.Preset)
		if err != nil {
			return nil, fmt.Errorf("invalid security preset %q: %w", cfg.Preset, err)
		}
		// 只有在独立开关未显式设置时才使用 preset
		if !cfg.VerifyPoW && !cfg.VerifyIndex && !cfg.VerifyReferenceChain && !cfg.VerifyIntegrity {
			verifyPoW = presetConfig.VerifyPoW
			verifyIndex = presetConfig.VerifyIndex
			verifyRef = presetConfig.VerifyReferenceChain
			verifyIntegrity = presetConfig.VerifyIntegrity
		}
	}

	bridge := NewBridge(verifyPoW, verifyIndex, verifyRef, verifyIntegrity)

	// 创建下载器
	dlCfg := download.DefaultFetcherConfig()
	dlCfg.CacheDir = cfg.CacheDir
	dlCfg.MaxFileSize = cfg.MaxFileSize
	if len(cfg.GatewayURLs) > 0 {
		dlCfg.Gateway = download.NewGateway(download.GatewayConfig{
			URLs:       cfg.GatewayURLs,
			Timeout:    30 * time.Second,
			MaxRetries: 3,
			RetryDelay: 1 * time.Second,
		})
	}
	fetcher := download.NewFetcher(dlCfg)

	// 并发控制信号量
	downloadMax := cfg.DownloadMaxConcurrency
	if downloadMax <= 0 {
		downloadMax = 2
	}
	onlineMax := cfg.OnlineMaxConcurrency
	if onlineMax <= 0 {
		onlineMax = 4
	}

	ctx, cancel := context.WithCancel(context.Background())

	svc := &Service{
		config:       cfg,
		bridge:       bridge,
		fetcher:      fetcher,
		downloadSema: make(chan struct{}, downloadMax),
		onlineSema:   make(chan struct{}, onlineMax),
		ctx:          ctx,
		cancel:       cancel,
		stats: ServiceStats{
			StartedAt: time.Now(),
		},
	}

	// ============================================================
	// Bitswap 按需拉取架构初始化
	// ============================================================
	if cfg.BitswapEnabled {
		if err := svc.initBitswap(cfg); err != nil {
			log.Warn("桥接服务：Bitswap 初始化失败（非致命）: %v", err)
		}
	}

	// 初始化 DHT Provider
	if cfg.DHTEnabled {
		if err := svc.initDHTProvider(); err != nil {
			log.Warn("桥接服务：DHT Provider 初始化失败（非致命）: %v", err)
		}
	}

	// 根据 DiscoveryMode 创建对应的 BlockChecker
	switch cfg.DiscoveryMode {
	case "graphql":
		gwURL := "https://arweave.net"
		if len(cfg.GatewayURLs) > 0 {
			gwURL = cfg.GatewayURLs[0]
		}
		checkerCfg := discovery.DefaultGraphQLCheckerConfig()
		checkerCfg.GatewayURL = gwURL
		checkerCfg.Timeout = cfg.PollInterval
		if checkerCfg.Timeout <= 0 {
			checkerCfg.Timeout = 30 * time.Second
		}
		graphqlChecker := discovery.NewGraphQLBlockChecker(checkerCfg)
		svc.SetBlockChecker(graphqlChecker)
		log.Info("桥接服务：发现模式 = graphql (网关: %s)", gwURL)
	case "sampling", "":
		// 随机抽样模式：不预设 checker，由外部通过 SetBlockChecker 注入
		log.Info("桥接服务：发现模式 = sampling（等待外部注入 BlockChecker）")
	default:
		log.Warn("桥接服务：未知发现模式 %q，使用 sampling 模式", cfg.DiscoveryMode)
	}

	log.Info("桥接服务：初始化完成 preset=%s verify_pow=%v verify_index=%v verify_ref=%v verify_integrity=%v online_verify=%v dht=%v discovery=%s",
		cfg.Preset, verifyPoW, verifyIndex, verifyRef, verifyIntegrity, cfg.OnlineVerify, cfg.DHTEnabled && svc.dhtProvider != nil, cfg.DiscoveryMode)

	return svc, nil
}

// getOnlineVerifier 延迟初始化在线验证器（共享网关连接）
func (s *Service) getOnlineVerifier() *verify.OnlineVerifier {
	s.onlineVerifierOnce.Do(func() {
		s.onlineVerifier = verify.NewOnlineVerifier(verify.OnlineVerifierConfig{
			Gateway:        s.fetcher.GetGateway(),
			SampleCount:    s.config.OnlineSampleCount,
			MaxConcurrency: s.config.OnlineMaxConcurrency,
		})
	})
	return s.onlineVerifier
}

// initDHTProvider 初始化 DHT 内容发布提供器
//
// 如果配置了 DHTListenAddresses，会自动创建 libp2p host 并启动 DHT Provider。
// 否则只记录配置，由外部通过 SetDHTProvider 注入 Provider。
func (s *Service) initDHTProvider() error {
	cfg := dht.DefaultConfig()
	cfg.Enabled = true

	if s.config.DHTMode != "" {
		cfg.Mode = dht.Mode(s.config.DHTMode)
	}
	if len(s.config.DHTBootstrapPeers) > 0 {
		cfg.BootstrapPeers = s.config.DHTBootstrapPeers
	}
	if s.config.DHTProvideConcurrency > 0 {
		cfg.ProvideConcurrency = s.config.DHTProvideConcurrency
	}
	if s.config.DHTReprovideInterval != "" {
		if dur, err := time.ParseDuration(s.config.DHTReprovideInterval); err == nil {
			cfg.ReprovideInterval = dur
		} else {
			log.Warn("桥接服务：无法解析 DHT 重提供间隔 %q: %v", s.config.DHTReprovideInterval, err)
		}
	}

	// 如果配置了监听地址，自动创建 host 和 provider
	if len(s.config.DHTListenAddresses) > 0 {
		hostCfg := dht.DefaultHostConfig()
		hostCfg.ListenAddresses = s.config.DHTListenAddresses
		if len(cfg.BootstrapPeers) > 0 {
			hostCfg.BootstrapPeers = cfg.BootstrapPeers
		}

		provider, _, err := dht.NewProviderWithHost(hostCfg, cfg)
		if err != nil {
			return fmt.Errorf("创建 DHT Provider (含 host) 失败: %w", err)
		}
		s.dhtProvider = provider
		log.Info("桥接服务：DHT Provider 已自动创建并启动")
	} else {
		log.Info("桥接服务：DHT Provider 配置: mode=%s concurrency=%d reprovide=%s（等待外部注入 host）",
			cfg.Mode, cfg.ProvideConcurrency, cfg.ReprovideInterval)
		s.dhtProvider = nil // 由外部通过 SetDHTProvider 注入
	}

	return nil
}

// initBitswap 初始化 Bitswap 按需拉取架构
//
// 创建链路：Badger DB → index.Store → BlockFetcher → bitswap.Service
func (s *Service) initBitswap(cfg ServiceConfig) error {
	// 1. 创建 Badger DB（复用 store 配置）
	badgerCfg := store.DefaultStoreConfig()
	badgerCfg.Dir = cfg.CacheDir + "/badger"
	s.badgerDB = nil // 延迟初始化，避免重复创建

	// 如果已有 store 实例，复用其 Badger DB
	if s.cacheStore == nil {
		var err error
		s.cacheStore, err = store.New(badgerCfg)
		if err != nil {
			return fmt.Errorf("创建 Badger 存储失败: %w", err)
		}
	}

	// 使用 cacheStore 的底层 Badger DB（需要通过反射或导出方法获取）
	// 暂时创建独立的 Badger 实例用于索引
	indexBadgerCfg := badger.DefaultOptions(cfg.CacheDir + "/index")
	indexBadgerCfg.MemTableSize = 16 << 20
	indexBadgerCfg.NumMemtables = 2
	indexBadgerCfg.BlockCacheSize = 8 << 20
	indexBadgerCfg.IndexCacheSize = 4 << 20
	indexBadgerCfg.ValueLogFileSize = 32 << 20
	indexBadgerCfg.Logger = nil

	db, err := badger.Open(indexBadgerCfg)
	if err != nil {
		return fmt.Errorf("打开索引 Badger 失败: %w", err)
	}
	s.badgerDB = db

	// 2. 创建索引存储
	s.indexStore = index.NewStore(db)
	log.Info("桥接服务：索引存储已初始化")

	// 3. 创建 Bitswap 块缓存
	cacheSize := cfg.CacheSize
	if cacheSize <= 0 {
		cacheSize = 1 << 30 // 默认 1 GB
	}
	blockCacheCfg := cache.CacheConfig{
		Dir:     cfg.CacheDir + "/blocks",
		MaxSize: cacheSize,
	}
	blockCache, err := cache.New(blockCacheCfg)
	if err != nil {
		return fmt.Errorf("创建块缓存失败: %w", err)
	}

	// 4. 创建按需拉取器
	bfCfg := BlockFetcherConfig{
		IndexStore:             s.indexStore,
		Cache:                  blockCache,
		Gateway:                s.fetcher.GetGateway(),
		MaxConcurrentDownloads: cfg.DownloadMaxConcurrency,
	}
	if bfCfg.MaxConcurrentDownloads <= 0 {
		bfCfg.MaxConcurrentDownloads = 4
	}

	s.blockFetcher, err = NewBlockFetcher(bfCfg)
	if err != nil {
		return fmt.Errorf("创建 BlockFetcher 失败: %w", err)
	}

	// 5. 创建 Bitswap 服务
	bitswapPort := cfg.BitswapPort
	if bitswapPort <= 0 {
		bitswapPort = 4001
	}
	bitswapCfg := bitswap.DefaultConfig()
	bitswapCfg.ListenAddr = fmt.Sprintf(":%d", bitswapPort)
	bitswapCfg.DelayedReply = true
	bitswapCfg.Cache = blockCache
	bitswapCfg.Timeout = 30 * time.Second

	s.bitswapService, err = bitswap.New(bitswapCfg)
	if err != nil {
		return fmt.Errorf("创建 Bitswap 服务失败: %w", err)
	}

	// 6. 注册 BlockFetcher 到 Bitswap
	s.bitswapService.SetBlockFetcher(s.blockFetcher)

	log.Info("桥接服务：Bitswap 按需拉取架构已初始化 port=%d cache_size=%dMB",
		bitswapPort, cacheSize>>20)
	return nil
}

// SetDHTProvider 设置 DHT Provider（在 libp2p host 初始化后调用）
func (s *Service) SetDHTProvider(provider *dht.Provider) {
	s.dhtProvider = provider
}

// GetDHTProvider 获取 DHT Provider
func (s *Service) GetDHTProvider() *dht.Provider {
	return s.dhtProvider
}

// Start 启动桥接服务（阻塞）
func (s *Service) Start() error {
	log.Info("桥接服务：启动中...")
	log.Info("  安全预设: %s", s.config.Preset)
	log.Info("  缓存目录: %s", s.config.CacheDir)
	log.Info("  最大文件: %d bytes", s.config.MaxFileSize)
	log.Info("  在线验证: %v", s.config.OnlineVerify)
	log.Info("  下载并发: %d | 在线验证并发: %d",
		cap(s.downloadSema), cap(s.onlineSema))
	if s.bitswapService != nil {
		log.Info("  Bitswap: 已启用 (端口 %d)", s.config.BitswapPort)
	} else {
		log.Info("  Bitswap: 未启用")
	}
	if s.dhtProvider != nil {
		log.Info("  DHT 内容发布: 已启用")
	} else if s.config.DHTEnabled {
		log.Info("  DHT 内容发布: 已配置但 Provider 未注入")
	} else {
		log.Info("  DHT 内容发布: 未启用")
	}

	// 启动发现采样器（如果配置了 checker）
	if s.sampler != nil {
		s.sampler.StartPolling()
	}

	// 主循环
	s.runMainLoop()

	return nil
}

// StartAsync 异步启动桥接服务
func (s *Service) StartAsync() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.Start(); err != nil {
			log.Error("桥接服务错误: %v", err)
		}
	}()
}

// Stop 优雅停止桥接服务
func (s *Service) Stop() {
	log.Info("桥接服务：正在停止...")
	s.cancel()
	if s.sampler != nil {
		s.sampler.Stop()
	}
	// 停止 Bitswap 服务
	if s.bitswapService != nil {
		if err := s.bitswapService.Close(); err != nil {
			log.Warn("桥接服务：停止 Bitswap 失败: %v", err)
		}
	}
	// 停止 DHT Provider
	if s.dhtProvider != nil {
		if err := s.dhtProvider.Stop(); err != nil {
			log.Warn("桥接服务：停止 DHT Provider 失败: %v", err)
		}
	}
	// 关闭 Badger 数据库
	if s.badgerDB != nil {
		if err := s.badgerDB.Close(); err != nil {
			log.Warn("桥接服务：关闭 Badger 失败: %v", err)
		}
	}
	// 关闭 KV 存储
	if s.cacheStore != nil {
		if err := s.cacheStore.Close(); err != nil {
			log.Warn("桥接服务：关闭 KV 存储失败: %v", err)
		}
	}
	s.wg.Wait()
	log.Info("桥接服务：已停止")
}

// SetSampler 设置发现采样器
func (s *Service) SetSampler(sampler *discovery.Sampler) {
	s.sampler = sampler
}

// SetBlockChecker 设置区块检查器并创建采样器
func (s *Service) SetBlockChecker(checker discovery.BlockChecker) {
	cfg := discovery.SamplerConfig{
		MinHeight:    s.config.MinBlockHeight,
		MaxHeight:    s.config.MaxBlockHeight,
		PollInterval: s.config.PollInterval,
		MaxChecked:   1_000_000,
	}
	s.sampler = discovery.NewSampler(cfg, checker)
}

// runMainLoop 运行主循环
func (s *Service) runMainLoop() {
	// 如果没有配置采样器，运行一次后返回
	if s.sampler == nil {
		log.Info("桥接服务：未配置发现采样器，服务以被动模式运行")
		// 阻塞直到取消
		<-s.ctx.Done()
		return
	}

	log.Info("桥接服务：主循环已启动，开始发现数据...")

	pollTicker := time.NewTicker(s.config.PollInterval)
	defer pollTicker.Stop()

	// 首次立即执行
	s.pollAndProcess()

	for {
		select {
		case <-pollTicker.C:
			s.pollAndProcess()
		case <-s.ctx.Done():
			return
		}
	}
}

// pollAndProcess 轮询并处理发现的区块
func (s *Service) pollAndProcess() {
	// 随机抽样
	result, err := s.sampler.Sample()
	if err != nil {
		log.Warn("桥接服务：抽样失败: %v", err)
		return
	}

	s.updateActivity()

	if result.Exhausted {
		log.Debug("桥接服务：抽样范围已耗尽")
		return
	}

	s.mu.Lock()
	s.stats.BlocksChecked++
	s.mu.Unlock()

	if !result.Found {
		return
	}

	s.mu.Lock()
	s.stats.BlocksFound++
	s.mu.Unlock()

	log.Info("桥接服务：发现数据区块 高度=%d 元数据交易数=%d", result.Height, len(result.MetadataTXIDs))

	// 处理每个发现的元数据交易
	for _, txID := range result.MetadataTXIDs {
		if _, err := s.processMetadataTX(txID); err != nil {
			log.Warn("桥接服务：处理元数据交易 %s 失败: %v", txID, err)
		}
	}
}

// ProcessMetadataTX 手动处理一个元数据交易（公开接口）
// 根据配置选择两条路径之一：
//
//	路径 A：在线验证（OnlineVerify=true）—— 不存盘，HTTP Range 采样验证
//	路径 B：缓存到磁盘（CarAvailable=true）—— 下载完整 CAR 文件
func (s *Service) ProcessMetadataTX(txID string) (*PipelineResult, error) {
	return s.processMetadataTX(txID)
}

// processMetadataTX 处理元数据交易（内部实现）
func (s *Service) processMetadataTX(txID string) (*PipelineResult, error) {
	log.Info("桥接服务：处理元数据交易 %s", txID)

	// Step 1: 获取元数据
	meta, err := s.fetcher.FetchMetadataByTXID(txID)
	if err != nil {
		return nil, fmt.Errorf("获取元数据失败: %w", err)
	}

	s.mu.Lock()
	s.stats.MetadataFetched++
	s.mu.Unlock()

	// Step 1.5: 如果是同 Bundle 模式，缓存 Bundle 原始数据
	// 需要重新获取 Bundle 交易数据（包含所有 Items），并提取 CAR Item
	if meta.IsSameBundle() {
		s.cacheSameBundleData(txID, meta)
	}

	// ============================================================
	// Bitswap 按需拉取路径：索引 CID，不下载完整 CAR
	// ============================================================
	if s.bitswapService != nil && s.indexStore != nil {
		s.indexMetadataCIDs(txID, meta)
		// Bitswap 模式：索引完成后，依赖 Bitswap 按需拉取
		// 仍然运行快速验证（元数据 + PoW）
	}

	// 元数据已在 FetchMetadataByTXID 中校验过
	// Step 2: 运行快速验证（元数据校验 + PoW）
	quickResult := s.bridge.RunPipeline(meta, false)

	if !quickResult.Passed {
		log.Warn("桥接服务：快速验证失败 root_cid=%s", meta.RootCID)
		for _, r := range quickResult.Results {
			if !r.Passed && !r.Skipped {
				log.Warn("  - %s: %s", r.Step, r.Error)
			}
		}
		s.mu.Lock()
		s.stats.PipelinesFailed++
		s.mu.Unlock()

		return &PipelineResult{
			Meta:       meta,
			Passed:     false,
			Steps:      quickResult.Results,
			CARPath:    "",
			QuickOnly:  true,
			VerifyMode: "quick",
		}, nil
	}

	// Step 3: 选择验证路径
	if s.config.OnlineVerify && meta.DataTXID != "" {
		// ====================
		// 路径 A：在线验证（不存盘）
		// ====================
		return s.processOnlineVerify(meta, quickResult)
	}

	if s.config.CarAvailable {
		// ====================
		// 路径 B：完整下载 + 缓存
		// ====================
		return s.processFullDownload(meta, quickResult)
	}

	// 两条路径都未启用，仅返回快速验证结果
	s.mu.Lock()
	if quickResult.Passed {
		s.stats.PipelinesPassed++
	} else {
		s.stats.PipelinesFailed++
	}
	s.mu.Unlock()

	return &PipelineResult{
		Meta:       meta,
		Passed:     quickResult.Passed,
		Steps:      quickResult.Results,
		CARPath:    "",
		QuickOnly:  true,
		VerifyMode: "quick",
	}, nil
}

// processOnlineVerify 路径 A：在线验证（HTTP Range 采样，不存盘）
func (s *Service) processOnlineVerify(meta *sdkmeta.Metadata, quickResult *pipeline.PipelineResult) (*PipelineResult, error) {
	log.Info("桥接服务：路径 A（在线验证）data_txid=%s", meta.DataTXID)

	// 获取并发信号量
	s.onlineSema <- struct{}{}
	defer func() { <-s.onlineSema }()

	verifier := s.getOnlineVerifier()
	onlineResult, err := verifier.VerifyWithMeta(meta)
	if err != nil {
		log.Warn("桥接服务：在线验证失败 data_txid=%s: %v", meta.DataTXID, err)

		s.mu.Lock()
		s.stats.PipelinesFailed++
		s.mu.Unlock()

		// 在线验证失败，构建失败的结果
		steps := append(quickResult.Results, pipeline.VerifyResult{
			Step:    pipeline.StepIndex,
			Passed:  false,
			Skipped: false,
			Error:   fmt.Sprintf("在线验证失败: %v", err),
		})

		return &PipelineResult{
			Meta:       meta,
			Passed:     false,
			Steps:      steps,
			CARPath:    "",
			VerifyMode: "online",
		}, nil
	}

	s.mu.Lock()
	s.stats.OnlineVerified++
	s.mu.Unlock()

	// 将在线验证结果与快速验证结果合并
	steps := quickResult.Results

	indexStep := pipeline.VerifyResult{
		Step:   pipeline.StepIndex,
		Passed: onlineResult.Passed,
	}
	if !onlineResult.Passed {
		indexStep.Error = fmt.Sprintf("在线采样验证未通过: verified=%d failed=%d total=%d",
			onlineResult.VerifiedBlocks, onlineResult.FailedBlocks, onlineResult.TotalBlocks)
		indexStep.Skipped = false
	} else {
		indexStep.Message = fmt.Sprintf("在线采样验证通过: %d/%d blocks (索引 %d blocks)",
			onlineResult.VerifiedBlocks, onlineResult.SampledBlocks, onlineResult.TotalBlocks)
	}
	steps = append(steps, indexStep)

	// 如果在线验证通过且配置了引用链或完整性验证，从在线验证获得的信息标记它们
	if s.bridge.config.VerifyReferenceChain && meta.HasReference() {
		// 引用链验证仍需完整数据或额外请求
		steps = append(steps, pipeline.VerifyResult{
			Step:    pipeline.StepReferenceChain,
			Passed:  true,
			Skipped: true,
			Message: "在线验证模式下引用链验证待实现",
		})
	}
	if s.bridge.config.VerifyIntegrity {
		// 完整性验证在在线模式下通过采样已部分覆盖
		steps = append(steps, pipeline.VerifyResult{
			Step:    pipeline.StepIntegrity,
			Passed:  true,
			Skipped: true,
			Message: "在线验证模式下完整性由采样覆盖",
		})
	}

	finalPassed := quickResult.Passed && onlineResult.Passed

	if finalPassed {
		s.mu.Lock()
		s.stats.PipelinesPassed++
		s.mu.Unlock()
		log.Info("桥接服务：在线验证通过 ✅ root_cid=%s verified=%d/%d",
			meta.RootCID, onlineResult.VerifiedBlocks, onlineResult.SampledBlocks)
	} else {
		s.mu.Lock()
		s.stats.PipelinesFailed++
		s.mu.Unlock()
		log.Warn("桥接服务：在线验证失败 ❌ root_cid=%s", meta.RootCID)
	}

	return &PipelineResult{
		Meta:       meta,
		Passed:     finalPassed,
		Steps:      steps,
		CARPath:    "",
		VerifyMode: "online",
	}, nil
}

// processFullDownload 路径 B：完整下载 CAR 文件到本地缓存
func (s *Service) processFullDownload(meta *sdkmeta.Metadata, quickResult *pipeline.PipelineResult) (*PipelineResult, error) {
	log.Info("桥接服务：路径 B（完整下载）data_txid=%s", meta.DataTXID)

	// 获取下载并发信号量
	s.downloadSema <- struct{}{}
	defer func() { <-s.downloadSema }()

	carPath, err := s.fetcher.DownloadCAR(meta)
	if err != nil {
		log.Warn("桥接服务：CAR 下载失败 root_cid=%s: %v", meta.RootCID, err)
		// 继续用快速验证结果
		return &PipelineResult{
			Meta:       meta,
			Passed:     quickResult.Passed,
			Steps:      quickResult.Results,
			CARPath:    "",
			QuickOnly:  true,
			VerifyMode: "download",
		}, nil
	}

	s.mu.Lock()
	s.stats.CARsDownloaded++
	s.mu.Unlock()

	log.Info("桥接服务：CAR 文件已下载 root_cid=%s path=%s", meta.RootCID, carPath)

	// DHT 内容发布：验证通过后发布 CID 到 DHT 网络
	if s.dhtProvider != nil && s.dhtProvider.IsStarted() {
		go func() {
			rootCID, err := cid.Decode(meta.RootCID)
			if err != nil {
				log.Warn("桥接服务：无法解析 RootCID %q 为 CID: %v", meta.RootCID, err)
				return
			}
			if err := s.dhtProvider.Provide(rootCID); err != nil {
				log.Warn("桥接服务：DHT Provide 失败 %s: %v", meta.RootCID, err)
			}
		}()
	}

	// 运行完整验证管道（索引、引用链、完整性）
	fullResult := s.bridge.RunPipeline(meta, true)

	// 记录结果
	if fullResult.Passed {
		s.mu.Lock()
		s.stats.PipelinesPassed++
		s.mu.Unlock()

		log.Info("桥接服务：验证管道通过 ✅ root_cid=%s steps=%d car=%s",
			meta.RootCID, len(fullResult.Results), carPath)
	} else {
		s.mu.Lock()
		s.stats.PipelinesFailed++
		s.mu.Unlock()

		log.Warn("桥接服务：验证管道失败 ❌ root_cid=%s", meta.RootCID)
		for _, r := range fullResult.Results {
			if !r.Passed && !r.Skipped {
				log.Warn("  - %s: %s", r.Step, r.Error)
			}
		}
	}

	// 清理缓存（可选）
	if !fullResult.Passed && carPath != "" {
		// 验证失败，清理 CAR 缓存
		os.Remove(carPath)
		carPath = ""
	}

	return &PipelineResult{
		Meta:       meta,
		Passed:     fullResult.Passed,
		Steps:      fullResult.Results,
		CARPath:    carPath,
		VerifyMode: "download",
	}, nil
}

// ProcessMetadataFromJSON 处理 JSON 格式的元数据
func (s *Service) ProcessMetadataFromJSON(jsonData []byte) (*PipelineResult, error) {
	meta, err := s.fetcher.FetchMetadataFromJSON(jsonData)
	if err != nil {
		return nil, fmt.Errorf("解析元数据 JSON 失败: %w", err)
	}

	s.mu.Lock()
	s.stats.MetadataFetched++
	s.mu.Unlock()

	return s.verifyMetadata(meta)
}

// ProcessMetadataFromBase64 处理 Base64URL 编码的元数据
func (s *Service) ProcessMetadataFromBase64(encoded string) (*PipelineResult, error) {
	meta, err := s.fetcher.FetchMetadataFromBase64(encoded)
	if err != nil {
		return nil, fmt.Errorf("解析元数据 Base64URL 失败: %w", err)
	}

	s.mu.Lock()
	s.stats.MetadataFetched++
	s.mu.Unlock()

	return s.verifyMetadata(meta)
}

// verifyMetadata 验证已解析的元数据（内部方法）
func (s *Service) verifyMetadata(meta *sdkmeta.Metadata) (*PipelineResult, error) {
	// 快速验证
	quickResult := s.bridge.RunPipeline(meta, false)
	if !quickResult.Passed {
		s.mu.Lock()
		s.stats.PipelinesFailed++
		s.mu.Unlock()

		return &PipelineResult{
			Meta:       meta,
			Passed:     false,
			Steps:      quickResult.Results,
			QuickOnly:  true,
			VerifyMode: "quick",
		}, nil
	}

	// 选择验证路径
	if s.config.OnlineVerify && meta.DataTXID != "" {
		return s.processOnlineVerify(meta, quickResult)
	}

	if s.config.CarAvailable {
		return s.processFullDownload(meta, quickResult)
	}

	// 都不启用，仅快速验证
	s.mu.Lock()
	if quickResult.Passed {
		s.stats.PipelinesPassed++
	} else {
		s.stats.PipelinesFailed++
	}
	s.mu.Unlock()

	return &PipelineResult{
		Meta:       meta,
		Passed:     quickResult.Passed,
		Steps:      quickResult.Results,
		QuickOnly:  true,
		VerifyMode: "quick",
	}, nil
}

// Stats 获取服务统计
func (s *Service) Stats() ServiceStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.stats
}

// updateActivity 更新最后活动时间
func (s *Service) updateActivity() {
	s.mu.Lock()
	s.stats.LastActivity = time.Now()
	s.mu.Unlock()
}

// GetFetcher 获取下载器（用于外部访问）
func (s *Service) GetFetcher() *download.Fetcher {
	return s.fetcher
}

// cacheSameBundleData 缓存同 Bundle 的 CAR 数据
// 当 data_height = -1 且 bundle_txid = "none" 时，
// 元数据和 CAR 文件在同一个 Bundle 内。
// 此方法从 Bundle 交易中提取 CAR Item 的纯数据并缓存。
func (s *Service) cacheSameBundleData(metadataTxID string, meta *sdkmeta.Metadata) {
	gateway := s.fetcher.GetGateway()
	if gateway == nil {
		log.Warn("桥接服务：无法缓存同 Bundle 数据，网关不可用")
		return
	}

	// 获取 Bundle 原始数据
	// metadataTxID 是元数据所在的交易 ID（可能是 Bundle TXID 或 Bundle Item ID）
	// 这里简化处理：假设 metadataTxID 就是 Bundle TXID
	_, rawData, err := gateway.FetchBundleItemByID(metadataTxID, meta.DataTXID)
	if err != nil {
		log.Warn("桥接服务：缓存同 Bundle 数据失败 bundle=%s item=%s: %v",
			metadataTxID, meta.DataTXID, err)
		return
	}

	// 缓存到 Fetcher（用于下载）
	s.fetcher.CacheBundleRawData(meta.DataTXID, rawData)

	// 同时缓存到 OnlineVerifier（用于在线验证）
	if s.config.OnlineVerify {
		s.getOnlineVerifier().SetBundleData(meta.DataTXID, rawData)
	}

	log.Info("桥接服务：同 Bundle 数据已缓存 item=%s size=%d", meta.DataTXID, len(rawData))
}

// GetOnlineVerifier 获取在线验证器
func (s *Service) GetOnlineVerifier() *verify.OnlineVerifier {
	return s.getOnlineVerifier()
}

// ============================================================
// Bitswap 按需拉取：CID 索引
// ============================================================

// indexMetadataCIDs 将元数据中的 CID 信息写入索引
//
// 为 Bitswap 按需拉取做准备：
//   - 索引 RootCID → Arweave 交易位置
//   - 索引 Reference 中的 CID → 对应 Arweave 交易位置
func (s *Service) indexMetadataCIDs(metadataTxID string, meta *sdkmeta.Metadata) {
	if s.indexStore == nil {
		return
	}

	// 索引 RootCID
	rootEntry := &index.Entry{
		CID:        meta.RootCID,
		DataTXID:   meta.DataTXID,
		BundleTXID: meta.BundleTXID,
		Height:     int64(meta.DataHeight),
		DataSize:   int64(meta.DataSize),
		RawIndex:   nil, // 暂无 CAR Index
	}
	if err := s.indexStore.Put(rootEntry); err != nil {
		log.Warn("桥接服务：索引 RootCID 失败 %s: %v", meta.RootCID, err)
	} else {
		log.Debug("桥接服务：已索引 RootCID %s → tx=%s", meta.RootCID, meta.DataTXID)
	}

	// 索引 Reference 中的 CID
	if meta.HasReference() {
		refMap := *meta.Reference
		for refTXID, entry := range refMap {
			for _, cidStr := range entry.CIDs {
				refEntry := &index.Entry{
					CID:        cidStr,
					DataTXID:   refTXID,
					BundleTXID: entry.BundleTXID,
					Height:     int64(entry.Height),
					DataSize:   0, // 引用条目不记录大小
					RawIndex:   nil,
				}
				if err := s.indexStore.Put(refEntry); err != nil {
					log.Warn("桥接服务：索引引用 CID 失败 %s: %v", cidStr, err)
				} else {
					log.Debug("桥接服务：已索引引用 CID %s → tx=%s", cidStr, refTXID)
				}
			}
		}
	}

	log.Info("桥接服务：CID 索引完成 root=%s", meta.RootCID)
}

// ============================================================
// Bitswap 相关公开方法
// ============================================================

// GetBitswapService 获取 Bitswap 服务
func (s *Service) GetBitswapService() *bitswap.Service {
	return s.bitswapService
}

// GetIndexStore 获取 CID 索引存储
func (s *Service) GetIndexStore() *index.Store {
	return s.indexStore
}

// GetBlockFetcher 获取按需拉取器
func (s *Service) GetBlockFetcher() *BlockFetcher {
	return s.blockFetcher
}

// SetBadgerDB 注入外部 Badger DB（用于共享存储）
// 在 initBitswap 之前调用以复用一个 Badger 实例
func (s *Service) SetBadgerDB(db *badger.DB) {
	s.badgerDB = db
}

// SetKVStore 注入外部 KV 存储
func (s *Service) SetKVStore(kvStore *store.Store) {
	s.cacheStore = kvStore
}

// ============================================================
// 类型定义
// ============================================================

// PipelineResult 管道处理结果（扩展版）
type PipelineResult struct {
	Meta       *sdkmeta.Metadata            `json:"meta"`
	Passed     bool                         `json:"passed"`
	Steps      []pipeline.VerifyResult      `json:"steps"`
	CARPath    string                       `json:"car_path,omitempty"`
	QuickOnly  bool                         `json:"quick_only,omitempty"`
	VerifyMode string                       `json:"verify_mode,omitempty"` // "quick", "online", "download"
}

// FormatResult 格式化管道结果为可读字符串
func FormatResult(result *PipelineResult) string {
	if result == nil {
		return "无结果"
	}

	status := "✅ 通过"
	if !result.Passed {
		status = "❌ 失败"
	}

	modeLabel := ""
	switch result.VerifyMode {
	case "online":
		modeLabel = " (在线验证)"
	case "download":
		modeLabel = " (完整下载)"
	case "quick":
		modeLabel = " (仅快速验证)"
	}

	output := fmt.Sprintf("验证结果: %s%s\n", status, modeLabel)
	output += "──────────────────────────────────────\n"

	if result.Meta != nil {
		output += fmt.Sprintf("  Root CID:  %s\n", result.Meta.RootCID)
		output += fmt.Sprintf("  Data TXID: %s\n", result.Meta.DataTXID)
		output += fmt.Sprintf("  Data Size: %d bytes\n", result.Meta.DataSize)
		output += fmt.Sprintf("  Method:    %s\n", result.Meta.Method)
		if result.Meta.HasReference() {
			output += fmt.Sprintf("  References: %d\n", len(*result.Meta.Reference))
		}
		output += "\n"
	}

	output += "验证步骤:\n"
	for _, step := range result.Steps {
		icon := "✅"
		if !step.Passed && !step.Skipped {
			icon = "❌"
		} else if step.Skipped {
			icon = "⏭️"
		}
		output += fmt.Sprintf("  %s %s", icon, step.Step)
		if step.Skipped {
			output += fmt.Sprintf(" (跳过: %s)", step.Message)
		} else if !step.Passed {
			output += fmt.Sprintf(" (失败: %s)", step.Error)
		} else if step.Message != "" {
			output += fmt.Sprintf(" (%s)", step.Message)
		}
		output += "\n"
	}

	if result.CARPath != "" {
		output += fmt.Sprintf("\n  CAR 缓存: %s\n", result.CARPath)
	}

	output += "──────────────────────────────────────\n"
	return output
}
