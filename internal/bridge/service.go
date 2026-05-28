// Package bridge 提供 IPFAR 桥接核心逻辑
package bridge

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	sdkArweave "github.com/LWDJD/ipfar-sdk/arweave"
	sdkmeta "github.com/LWDJD/ipfar-sdk/verify/metadata"
	"github.com/LWDJD/ipfar-sdk/verify/pipeline"
	"github.com/LWDJD/ipfar-sdk/verify/pow"
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
	DiscoveryMode  string // "sampling"（随机游走）、"graphql"（GraphQL 单块查询）、"graphql-scan"（顺序扫描，默认）
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
	OnlineVerify          bool
	OnlineSampleCount     int
	OnlineMaxConcurrency  int
	DownloadMaxConcurrency int

	// DHT 内容发布配置（规范 P3-1）
	DHTEnabled          bool
	DHTMode             string
	DHTBootstrapPeers   []string
	DHTReprovideInterval string
	DHTProvideConcurrency int
	DHTListenAddresses  []string

	// Bitswap 按需拉取配置
	BitswapEnabled bool
	BitswapPort    int
	CacheSize      int64

	// GraphQL 扫描配置（graphql-scan 模式）
	ScanBatchSize  int           // 每批扫描块数（默认 100）
	ScanQueryDelay time.Duration // 批次间延迟（默认 2s）

	// Debug 模式，开启后日志级别设为 debug
	Debug bool `json:"debug,omitempty"`
}

// DefaultServiceConfig 返回默认服务配置
func DefaultServiceConfig() ServiceConfig {
	return ServiceConfig{
		Preset:                 pipeline.SecurityLight,
		DiscoveryMode:          "graphql-scan", // 默认使用 GraphQL 顺序扫描
		MinBlockHeight:         1919626,
		MaxBlockHeight:         0, // 动态跟随
		PollInterval:           2 * time.Minute,
		CacheDir:               "cache/car",
		MaxFileSize:            0, // 不限制
		BitswapEnabled:         true,
		BitswapPort:            4001,
		CacheSize:              10 * 1024 * 1024 * 1024, // 10 GiB
		CarAvailable:           false,   // 按需拉取模式，不预下载
		OnlineVerify:           false,
		OnlineSampleCount:      5,
		OnlineMaxConcurrency:   4,
		DownloadMaxConcurrency: 2,
		ScanBatchSize:          100,
		ScanQueryDelay:         2 * time.Second,
		DHTEnabled:             true,
		DHTListenAddresses:     []string{"/ip4/0.0.0.0/tcp/4001"},
	}
}

// getVerifyConfig 从 ServiceConfig 中获取 pipeline.VerifyConfig
// 默认值与 pipeline.SecurityLight 一致：Index=true
func (c *ServiceConfig) getVerifyConfig() pipeline.VerifyConfig {
	verifyPoW := c.VerifyPoW
	verifyIndex := c.VerifyIndex
	verifyRef := c.VerifyReferenceChain
	verifyIntegrity := c.VerifyIntegrity

	// 如果设置了 preset，优先使用 preset
	if c.Preset != "" {
		presetConfig, err := pipeline.GetPreset(c.Preset)
		if err == nil {
			// 只有在独立开关未显式设置时才使用 preset
			if !c.VerifyPoW && !c.VerifyIndex && !c.VerifyReferenceChain && !c.VerifyIntegrity {
				verifyPoW = presetConfig.VerifyPoW
				verifyIndex = presetConfig.VerifyIndex
				verifyRef = presetConfig.VerifyReferenceChain
				verifyIntegrity = presetConfig.VerifyIntegrity
			}
		}
	}

	return pipeline.VerifyConfig{
		VerifyPoW:            verifyPoW,
		VerifyIndex:          verifyIndex,
		VerifyReferenceChain: verifyRef,
		VerifyIntegrity:      verifyIntegrity,
	}
}

// Service 桥接服务，集成发现 → 下载 → 验证完整链路
type Service struct {
	config  ServiceConfig
	bridge  *Bridge
	fetcher *download.Fetcher
	sampler *discovery.Sampler

	// 新架构：GraphQL 顺序扫描 + 区块监听
	graphQLScanner *discovery.GraphQLScanner
	blockWatcher   *discovery.BlockWatcher

	// DHT 内容发布提供器
	dhtProvider *dht.Provider

	// 在线验证器（延迟初始化）
	onlineVerifier     *verify.OnlineVerifier
	onlineVerifierOnce sync.Once

	// Bitswap 按需拉取架构
	bitswapService *bitswap.Service // Bitswap 服务端
	indexStore     *index.Store     // CID → Arweave 索引（两层）
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
	// 按需拉取模式：始终不预下载 CAR，CAR 下载由 Bitswap 按需触发
	cfg.CarAvailable = false

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

	// M5: 对配置的网关执行健康检查，不可用的自动跳过
	if len(cfg.GatewayURLs) > 0 && fetcher.GetGateway() != nil {
		healthyURLs := make([]string, 0, len(cfg.GatewayURLs))
		for _, gwURL := range cfg.GatewayURLs {
			client := sdkArweave.NewGatewayClient(gwURL)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := client.CheckHealth(ctx)
			cancel()
			if err != nil {
				log.Warn("桥接服务：网关健康检查失败 %s: %v，跳过此网关", gwURL, err)
			} else {
				log.Info("桥接服务：网关健康检查通过 %s", gwURL)
				healthyURLs = append(healthyURLs, gwURL)
			}
		}
		if len(healthyURLs) == 0 {
			log.Warn("桥接服务：所有网关健康检查均失败，使用默认网关列表")
			healthyURLs = []string{"https://arweave.net"}
		}
		// 更新 fetcher 网关配置
		cfg.GatewayURLs = healthyURLs
	}

	// B5: 注入 GatewayClient 到 Bridge（用于引用链验证等网络操作）
	if fetcher.GetGateway() != nil {
		// 获取底层 SDK GatewayClient（单网关，取第一个）
		gw := fetcher.GetGateway()
		if gw != nil && gw.MultiGatewayClient != nil {
			// 创建单网关的 GatewayClient 用于 pipeline 引用链验证
			// 使用 fetcher 内置网关的第一个 URL
			gwURLs := cfg.GatewayURLs
			if len(gwURLs) == 0 {
				gwURLs = []string{"https://arweave.net"}
			}
			sdkClient := sdkArweave.NewGatewayClient(gwURLs[0])
			bridge.SetGatewayClient(sdkClient)
		}
	}

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
			log.Warn("桥接服务：Bitswap 初始化失败（非致命），服务将以降级模式运行，不使用 Bitswap 按需拉取: %v", err)
		}
	}

	// ============================================================
	// 初始化 DHT Provider
	// ============================================================
	if cfg.DHTEnabled {
		if err := svc.initDHTProvider(); err != nil {
			log.Warn("桥接服务：DHT Provider 初始化失败（非致命）: %v", err)
		}
	}

	// ============================================================
	// 根据 DiscoveryMode 创建相应的发现组件
	// ============================================================
	switch cfg.DiscoveryMode {
	case "graphql-scan":
		// 新：GraphQL 顺序扫描 + 区块监听
		if err := svc.initGraphQLScan(cfg); err != nil {
			log.Warn("桥接服务：GraphQL 扫描初始化失败: %v", err)
		}
		log.Info("桥接服务：发现模式 = graphql-scan（顺序扫描 + 新区块监听）")
	case "graphql":
		// 旧：单块 GraphQL 查询（通过采样器）
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
		log.Info("桥接服务：发现模式 = graphql（单块查询，网关: %s）", gwURL)
	case "sampling", "":
		// 旧：随机抽样 + GraphQL 检查
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
		blockChecker := discovery.NewGraphQLBlockChecker(checkerCfg)
		svc.SetBlockChecker(blockChecker)
		log.Info("桥接服务：发现模式 = sampling（随机抽样 + GraphQL 检查，网关: %s）", gwURL)
	default:
		log.Warn("桥接服务：未知发现模式 %q，使用 graphql-scan 模式", cfg.DiscoveryMode)
		if err := svc.initGraphQLScan(cfg); err != nil {
			log.Warn("桥接服务：GraphQL 扫描初始化失败: %v", err)
		}
	}

	log.Info("桥接服务：初始化完成 preset=%s verify_pow=%v verify_index=%v verify_ref=%v verify_integrity=%v online_verify=%v dht=%v discovery=%s debug=%v",
		cfg.Preset, verifyPoW, verifyIndex, verifyRef, verifyIntegrity, cfg.OnlineVerify, cfg.DHTEnabled && svc.dhtProvider != nil, cfg.DiscoveryMode, cfg.Debug)

	return svc, nil
}

// initGraphQLScan 初始化 GraphQL 顺序扫描 + 区块监听
func (s *Service) initGraphQLScan(cfg ServiceConfig) error {
	// 确保索引存储已创建
	if s.indexStore == nil {
		return fmt.Errorf("索引存储未初始化，请确保 Bitswap 已启用或手动创建索引")
	}

	gwURL := "https://arweave.net"
	if len(cfg.GatewayURLs) > 0 {
		gwURL = cfg.GatewayURLs[0]
	}

	// 1. 创建 GraphQL 顺序扫描器
	scannerCfg := discovery.GraphQLScannerConfig{
		GatewayURL: gwURL,
		MinHeight:  cfg.MinBlockHeight,
		MaxHeight:  cfg.MaxBlockHeight,
		BatchSize:  cfg.ScanBatchSize,
		QueryDelay: cfg.ScanQueryDelay,
		Index:      s.indexStore,
	}
	if scannerCfg.BatchSize <= 0 {
		scannerCfg.BatchSize = 100
	}
	if scannerCfg.QueryDelay <= 0 {
		scannerCfg.QueryDelay = 2 * time.Second
	}

	var err error
	s.graphQLScanner, err = discovery.NewGraphQLScanner(scannerCfg)
	if err != nil {
		return fmt.Errorf("创建 GraphQL 扫描器失败: %w", err)
	}

	// 2. 创建区块监听器
	watcherCfg := discovery.BlockWatcherConfig{
		GatewayURL:   gwURL,
		PollInterval: 30 * time.Second,
		Index:        s.indexStore,
		Scanner:      s.graphQLScanner,
	}
	s.blockWatcher = discovery.NewBlockWatcher(watcherCfg)

	// 3. 注册回调：扫描器 / 监听器发现元数据后 → 触发 Service 完整 pipeline
	s.graphQLScanner.SetOnMetaFound(func(txID string, metaJSON []byte, height uint64) {
		s.processMetadataTX(txID)
	})
	s.blockWatcher.SetOnBlockFound(func(height uint64, metadataTXIDs []string) {
		for _, txID := range metadataTXIDs {
			s.processMetadataTX(txID)
		}
	})

	log.Info("桥接服务：GraphQL 顺序扫描 + 区块监听已初始化 minHeight=%d batchSize=%d",
		cfg.MinBlockHeight, scannerCfg.BatchSize)
	return nil
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
		s.dhtProvider = nil
	}

	return nil
}

// initBitswap 初始化 Bitswap 按需拉取架构
func (s *Service) initBitswap(cfg ServiceConfig) error {
	// 1. 创建 Badger DB
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

	// 2. 创建两层索引存储
	s.indexStore = index.NewStore(db)
	log.Info("桥接服务：两层索引存储已初始化")

	// 3. 创建 Bitswap 块缓存
	cacheSize := cfg.CacheSize
	if cacheSize <= 0 {
		cacheSize = 1 << 30
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
		StrictVerifier:         s.strictVerifyMetadata, // Bitswap 请求时升到 strict 级别验证
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

	maxPortAttempts := 5
	var lastErr error
	for attempt := 0; attempt < maxPortAttempts; attempt++ {
		listenAddr := fmt.Sprintf(":%d", bitswapPort+attempt)

		bitswapCfg := bitswap.DefaultConfig()
		bitswapCfg.ListenAddr = listenAddr
		bitswapCfg.DelayedReply = true
		bitswapCfg.Cache = blockCache
		bitswapCfg.Timeout = 30 * time.Second

		s.bitswapService, err = bitswap.New(bitswapCfg)
		if err == nil {
			if attempt > 0 {
				log.Warn("桥接服务：Bitswap 端口 %d 被占用，已自动切换至 %d",
					cfg.BitswapPort, bitswapPort+attempt)
			}
			break
		}
		lastErr = err
	}

	if s.bitswapService == nil {
		return fmt.Errorf("Bitswap 无法绑定端口（尝试了 %d 个端口）: %w",
			maxPortAttempts, lastErr)
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

// Start 启动桥接服务
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
	if s.graphQLScanner != nil {
		log.Info("  GraphQL 扫描: 已启用 (minHeight=%d)", s.config.MinBlockHeight)
	}
	if s.blockWatcher != nil {
		log.Info("  区块监听: 已启用")
	}

	// ============================================================
	// Bitswap 已在 New() 中自动启动监听（无需显式 Start）
	// ============================================================
	if s.bitswapService != nil {
		log.Info("桥接服务：Bitswap 已启动，监听地址=%s", s.bitswapService.ListenAddr())
	}

	// ============================================================
	// 启动 DHT Provider（已有）
	// ============================================================
	if s.dhtProvider != nil && !s.dhtProvider.IsStarted() {
		if err := s.dhtProvider.Start(); err != nil {
			log.Error("桥接服务：DHT Provider 启动失败: %v", err)
		}
	}

	// ============================================================
	// 启动 GraphQL 顺序扫描（新）—— 历史数据扫描
	// ============================================================
	if s.graphQLScanner != nil {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if err := s.graphQLScanner.Start(s.ctx); err != nil {
				log.Error("桥接服务：GraphQL 扫描器启动失败: %v", err)
			}
		}()
	}

	// ============================================================
	// 启动区块监听（新）—— 新区块发现
	// ============================================================
	if s.blockWatcher != nil {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if err := s.blockWatcher.Start(s.ctx); err != nil {
				log.Error("桥接服务：区块监听器启动失败: %v", err)
			}
		}()
	}

	// ============================================================
	// 旧模式兼容：启动采样器
	// ============================================================
	if s.sampler != nil {
		s.sampler.StartPolling()
		// 也运行主循环
		s.runMainLoop()
	} else if s.graphQLScanner != nil {
		// 新模式下阻塞等待取消
		log.Info("桥接服务：扫描/监听/发布/Bitswap 四大流程已启动，等待数据...")
		<-s.ctx.Done()
	} else {
		log.Info("桥接服务：未配置发现模式，以被动模式运行")
		<-s.ctx.Done()
	}

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

	// 停止采样器
	if s.sampler != nil {
		s.sampler.Stop()
	}

	// 停止 GraphQL 扫描器
	if s.graphQLScanner != nil {
		s.graphQLScanner.Stop()
	}

	// 停止区块监听器
	if s.blockWatcher != nil {
		s.blockWatcher.Stop()
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

// runMainLoop 运行主循环（旧采样模式）
func (s *Service) runMainLoop() {
	if s.sampler == nil {
		log.Info("桥接服务：未配置发现采样器，服务以被动模式运行")
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

// pollAndProcess 轮询并处理发现的区块（旧采样模式）
func (s *Service) pollAndProcess() {
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

	for _, txID := range result.MetadataTXIDs {
		if _, err := s.processMetadataTX(txID); err != nil {
			log.Warn("桥接服务：处理元数据交易 %s 失败: %v", txID, err)
		}
	}
}

// ProcessMetadataTX 手动处理一个元数据交易（公开接口）
func (s *Service) ProcessMetadataTX(txID string) (*PipelineResult, error) {
	return s.processMetadataTX(txID)
}

// processingTXIDs 正在处理的 txID 集合，防止重复处理
var processingTXIDs sync.Map // txID → bool

// processMetadataTX 处理元数据交易（内部实现）
func (s *Service) processMetadataTX(txID string) (*PipelineResult, error) {
	// 防重复处理：正在处理的 txID 不再触发第二次
	if _, loaded := processingTXIDs.LoadOrStore(txID, true); loaded {
		log.Debug("桥接服务：跳过已正在处理的 txID=%s", txID)
		return nil, nil
	}
	defer processingTXIDs.Delete(txID)

	log.Debug("processMetadataTX：开始处理 txID=%s", txID)
	log.Info("桥接服务：处理元数据交易 %s", txID)

	// 获取验证配置
	vcfg := s.config.getVerifyConfig()

	// Step 0: 按步骤验证缓存检查
	if s.indexStore != nil {
		log.Debug("验证缓存检查：txID=%s 步骤=[pow=%v, index=%v, ref_chain=%v, integrity=%v]",
			txID, vcfg.VerifyPoW, vcfg.VerifyIndex, vcfg.VerifyReferenceChain, vcfg.VerifyIntegrity)
		if s.allStepsCached(txID, vcfg) {
			log.Info("桥接服务：元数据所有步骤已验证通过，跳过 pipeline txID=%s", txID)
			return &PipelineResult{
				Meta:       nil,
				Passed:     true,
				QuickOnly:  true,
				VerifyMode: "cached",
			}, nil
		}
	}

	// Step 1: 获取元数据
	meta, err := s.fetcher.FetchMetadataByTXID(txID)
	if err != nil {
		return nil, fmt.Errorf("获取元数据失败: %w", err)
	}

	s.mu.Lock()
	s.stats.MetadataFetched++
	s.mu.Unlock()

	// Step 1.5: 如果是同 Bundle 模式，缓存 Bundle 原始数据
	if meta.IsSameBundle() {
		s.cacheSameBundleData(txID, meta)
	}

	// ============================================================
	// Bitswap 按需拉取路径：使用两层索引
	// ============================================================
	if s.indexStore != nil {
		s.indexMetadataCIDs(txID, meta)
	}

	// Step 2: 运行快速验证（使用去除了已缓存步骤的配置）
	quickResult := s.runPipelineWithCacheSkip(meta, false, txID)

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
			Incomplete: quickResult.Incomplete,
			Steps:      quickResult.Results,
			CARPath:    "",
			QuickOnly:  true,
			VerifyMode: "quick",
		}, nil
	}

	// DHT 发布：索引完成后立即发布所有 CID
	s.dhtProvideMetaCIDs(meta)

	// Step 3: 选择验证路径
	// 按需拉取模式：不预下载 CAR，只做轻量验证和索引
	// CAR 下载和完整验证在 Bitswap 请求时由 BlockFetcher 触发
	var result *PipelineResult
	if s.config.OnlineVerify && meta.DataTXID != "" {
		log.Debug("路径选择：onlineVerify=%v → 路径 A（在线验证）", s.config.OnlineVerify)
		result, _ = s.processOnlineVerify(meta, quickResult)
	} else {
		// 按需拉取：仅快速验证 + 索引，不下载 CAR
		log.Debug("路径选择：onlineVerify=%v → 路径 仅快速验证（按需拉取模式，CAR 下载由 Bitswap 触发）", s.config.OnlineVerify)
		s.mu.Lock()
		if quickResult.Passed {
			s.stats.PipelinesPassed++
		} else {
			s.stats.PipelinesFailed++
		}
		s.mu.Unlock()

		result = &PipelineResult{
			Meta:       meta,
			Passed:     quickResult.Passed,
			Incomplete: quickResult.Incomplete,
			Steps:      quickResult.Results,
			CARPath:    "",
			QuickOnly:  true,
			VerifyMode: "quick",
		}
	}

	// M2: 验证通过后按步骤标记已验证
	if result != nil && result.Passed && s.indexStore != nil {
		s.cacheStepResults(txID, result.Steps)
		// 同时保持向后兼容的 MarkVerified
		if err := s.indexStore.MarkVerified(txID); err != nil {
			log.Warn("桥接服务：标记已验证失败 txID=%s: %v", txID, err)
		}
	}

	return result, nil
}

// processOnlineVerify 路径 A：在线验证
func (s *Service) processOnlineVerify(meta *sdkmeta.Metadata, quickResult *pipeline.PipelineResult) (*PipelineResult, error) {
	log.Info("桥接服务：路径 A（在线验证）data_txid=%s", meta.DataTXID)

	s.onlineSema <- struct{}{}
	defer func() { <-s.onlineSema }()

	verifier := s.getOnlineVerifier()
	onlineResult, err := verifier.VerifyWithMeta(meta)
	if err != nil {
		log.Warn("桥接服务：在线验证失败 data_txid=%s: %v", meta.DataTXID, err)
		s.mu.Lock()
		s.stats.PipelinesFailed++
		s.mu.Unlock()

		steps := append(quickResult.Results, pipeline.VerifyResult{
			Step:    pipeline.StepIndex,
			Passed:  false,
			Skipped: false,
			Error:   fmt.Sprintf("在线验证失败: %v", err),
		})

		return &PipelineResult{
			Meta:       meta,
			Passed:     false,
			Incomplete: quickResult.Incomplete,
			Steps:      steps,
			CARPath:    "",
			VerifyMode: "online",
		}, nil
	}

	s.mu.Lock()
	s.stats.OnlineVerified++
	s.mu.Unlock()

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

	// B5: 在线验证路径中的引用链验证（不再硬编码跳过）
	incomplete := quickResult.Incomplete
	if s.bridge.config.VerifyReferenceChain && meta.HasReference() {
		refVR := s.verifyReferenceChainOnline(meta)
		steps = append(steps, refVR)
		if refVR.Incomplete {
			incomplete = true
		}
		if !refVR.Passed && !refVR.Skipped {
			// 引用链硬失败
			s.mu.Lock()
			s.stats.PipelinesFailed++
			s.mu.Unlock()
			return &PipelineResult{
				Meta:       meta,
				Passed:     false,
				Incomplete: incomplete,
				Steps:      steps,
				CARPath:    "",
				VerifyMode: "online",
			}, nil
		}
	}

	if s.bridge.config.VerifyIntegrity {
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

	// M4: DHT 发布（在线验证路径也触发）
	s.dhtProvideCID(meta.RootCID)

	return &PipelineResult{
		Meta:       meta,
		Passed:     finalPassed,
		Incomplete: incomplete,
		Steps:      steps,
		CARPath:    "",
		VerifyMode: "online",
	}, nil
}

// verifyReferenceChainOnline 在线验证路径中的引用链验证
// 使用 SDK pipeline 的引用链验证器（通过 GatewayClient 下载数据）
func (s *Service) verifyReferenceChainOnline(meta *sdkmeta.Metadata) pipeline.VerifyResult {
	// 创建独立的 pipeline 实例进行引用链验证
	refPipeline := pipeline.NewPipeline(s.bridge.config)
	if s.bridge.GetGatewayClient() != nil {
		refPipeline.SetGatewayClient(s.bridge.GetGatewayClient())
	} else {
		return pipeline.VerifyResult{
			Step:    pipeline.StepReferenceChain,
			Passed:  true,
			Skipped: true,
			Message: "在线验证模式下网关客户端不可用，引用链验证跳过",
		}
	}

	// 使用 executeStepWithIncomplete 模式执行引用链验证
	// 直接调用 pipeline 的引用验证器
	refVR, err := refPipeline.VerifyReferenceChain(meta)
	if err != nil {
		return pipeline.VerifyResult{
			Step:   pipeline.StepReferenceChain,
			Passed: false,
			Error:  fmt.Sprintf("引用链验证失败: %v", err),
		}
	}

	// 检查是否不完整
	if refVR.Incomplete {
		return pipeline.VerifyResult{
			Step:       pipeline.StepReferenceChain,
			Passed:     true,
			Incomplete: true,
			Message:    fmt.Sprintf("引用链不完整: %s", refVR.Message),
		}
	}

	if !refVR.Passed {
		return pipeline.VerifyResult{
			Step:   pipeline.StepReferenceChain,
			Passed: false,
			Error:  refVR.Error,
		}
	}

	return pipeline.VerifyResult{
		Step:    pipeline.StepReferenceChain,
		Passed:  true,
		Message: "引用链验证通过",
	}
}

// dhtProvideCID 统一 DHT 内容发布（M4）
func (s *Service) dhtProvideCID(rootCIDStr string) {
	if s.dhtProvider != nil && s.dhtProvider.IsStarted() && rootCIDStr != "" {
		go func() {
			rootCID, err := cid.Decode(rootCIDStr)
			if err != nil {
				log.Warn("桥接服务：无法解析 RootCID %q 为 CID: %v", rootCIDStr, err)
				return
			}
			if err := s.dhtProvider.Provide(rootCID); err != nil {
				log.Warn("桥接服务：DHT Provide 失败 %s: %v", rootCIDStr, err)
			}
		}()
	}
}

// dhtProvideMetaCIDs 从元数据中提取所有 CID 并通过 DHT 发布
// 在索引完成后调用，确保 DHT 网络能发现这些 CID
func (s *Service) dhtProvideMetaCIDs(meta *sdkmeta.Metadata) {
	if s.dhtProvider == nil || !s.dhtProvider.IsStarted() {
		log.Warn("桥接服务：DHT Provider 未启动，跳过 CID 发布")
		return
	}

	// 收集所有 CID：RootCID + 引用 CID
	var cidStrs []string
	if meta.RootCID != "" {
		cidStrs = append(cidStrs, meta.RootCID)
	}
	if meta.HasReference() {
		refMap := *meta.Reference
		for _, entry := range refMap {
			cidStrs = append(cidStrs, entry.CIDs...)
		}
	}

	for _, cidStr := range cidStrs {
		cidStr := cidStr
		go func() {
			parsedCID, err := cid.Decode(cidStr)
			if err != nil {
				log.Warn("桥接服务：无法解析 CID %q 用于 DHT 发布: %v", cidStr, err)
				return
			}
			if err := s.dhtProvider.Provide(parsedCID); err != nil {
				log.Warn("桥接服务：DHT Provide 失败 %s: %v", cidStr, err)
			} else {
				log.Debug("桥接服务：DHT 已发布 CID %s", cidStr)
			}
		}()
	}
}

// processFullDownload 路径 B：完整下载 CAR 文件
func (s *Service) processFullDownload(meta *sdkmeta.Metadata, quickResult *pipeline.PipelineResult) (*PipelineResult, error) {
	log.Info("桥接服务：路径 B（完整下载）data_txid=%s", meta.DataTXID)

	s.downloadSema <- struct{}{}
	defer func() { <-s.downloadSema }()

	carPath, err := s.fetcher.DownloadCAR(meta)
	if err != nil {
		log.Warn("桥接服务：CAR 下载失败 root_cid=%s: %v", meta.RootCID, err)
		return &PipelineResult{
			Meta:       meta,
			Passed:     quickResult.Passed,
			Incomplete: quickResult.Incomplete,
			Steps:      quickResult.Results,
			CARPath:    "",
			QuickOnly:  true,
			VerifyMode: "download",
		}, nil
	}

	// 读取 CAR 文件大小用于调试日志
	if carInfo, statErr := os.Stat(carPath); statErr == nil {
		log.Debug("CAR 下载完成：size=%d bytes path=%s", carInfo.Size(), carPath)
	}

	s.mu.Lock()
	s.stats.CARsDownloaded++
	s.mu.Unlock()

	log.Info("桥接服务：CAR 文件已下载 root_cid=%s path=%s", meta.RootCID, carPath)

	// M4: DHT 内容发布（完整下载路径）
	s.dhtProvideCID(meta.RootCID)

	fullResult := s.bridge.RunPipeline(meta, true)

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

	if !fullResult.Passed && carPath != "" {
		os.Remove(carPath)
		carPath = ""
	}

	return &PipelineResult{
		Meta:       meta,
		Passed:     fullResult.Passed,
		Incomplete: fullResult.Incomplete,
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
	quickResult := s.bridge.RunPipeline(meta, false)
	if !quickResult.Passed {
		s.mu.Lock()
		s.stats.PipelinesFailed++
		s.mu.Unlock()
		return &PipelineResult{
			Meta:       meta,
			Passed:     false,
			Incomplete: quickResult.Incomplete,
			Steps:      quickResult.Results,
			QuickOnly:  true,
			VerifyMode: "quick",
		}, nil
	}

	if s.config.OnlineVerify && meta.DataTXID != "" {
		return s.processOnlineVerify(meta, quickResult)
	}

	if s.config.CarAvailable {
		return s.processFullDownload(meta, quickResult)
	}

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
		Incomplete: quickResult.Incomplete,
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

// GetFetcher 获取下载器
func (s *Service) GetFetcher() *download.Fetcher {
	return s.fetcher
}

// cacheSameBundleData 缓存同 Bundle 的 CAR 数据
func (s *Service) cacheSameBundleData(metadataTxID string, meta *sdkmeta.Metadata) {
	gateway := s.fetcher.GetGateway()
	if gateway == nil {
		log.Warn("桥接服务：无法缓存同 Bundle 数据，网关不可用")
		return
	}

	_, rawData, err := gateway.FetchBundleItemByID(metadataTxID, meta.DataTXID)
	if err != nil {
		log.Warn("桥接服务：缓存同 Bundle 数据失败 bundle=%s item=%s: %v",
			metadataTxID, meta.DataTXID, err)
		return
	}

	s.fetcher.CacheBundleRawData(meta.DataTXID, rawData)

	if s.config.OnlineVerify {
		s.getOnlineVerifier().SetBundleData(meta.DataTXID, rawData)
	}

	log.Info("桥接服务：同 Bundle 数据已缓存 item=%s size=%d", meta.DataTXID, len(rawData))
}

// ============================================================
// 按步骤验证缓存辅助方法
// ============================================================

// allStepsCached 检查配置中所有启用的步骤是否均已缓存
func (s *Service) allStepsCached(txID string, vcfg pipeline.VerifyConfig) bool {
	if s.indexStore == nil {
		return false
	}

	// meta_validate 始终强制执行，检查其缓存状态
	if metaCached, _ := s.indexStore.IsStepVerified(txID, index.StepMetaValidate); !metaCached {
		return false
	}

	// 检查各主要步骤
	if vcfg.VerifyPoW {
		if cached, _ := s.indexStore.IsStepVerified(txID, index.StepPoW); !cached {
			return false
		}
	}
	if vcfg.VerifyIndex {
		if cached, _ := s.indexStore.IsStepVerified(txID, index.StepIndex); !cached {
			return false
		}
	}
	if vcfg.VerifyReferenceChain {
		if cached, _ := s.indexStore.IsStepVerified(txID, index.StepRefChain); !cached {
			return false
		}
	}
	if vcfg.VerifyIntegrity {
		if cached, _ := s.indexStore.IsStepVerified(txID, index.StepIntegrity); !cached {
			return false
		}
	}

	return true
}

// runPipelineWithCacheSkip 运行验证管道，跳过已缓存的步骤
// 通过构建临时 pipeline 配置，将已缓存的步骤标记为 disabled
func (s *Service) runPipelineWithCacheSkip(meta *sdkmeta.Metadata, carAvailable bool, txID string) *pipeline.PipelineResult {
	vcfg := s.config.getVerifyConfig()

	// 如果有索引存储，检查并跳过已缓存的步骤
	if s.indexStore != nil {
		// 构建去除已缓存步骤的配置
		runCfg := vcfg // 复制

		if vcfg.VerifyPoW {
			if cached, _ := s.indexStore.IsStepVerified(txID, index.StepPoW); cached {
				runCfg.VerifyPoW = false
				log.Debug("桥接服务：跳过已缓存的 PoW 验证 txID=%s", txID)
			}
		}
		if vcfg.VerifyIndex {
			if cached, _ := s.indexStore.IsStepVerified(txID, index.StepIndex); cached {
				runCfg.VerifyIndex = false
				log.Debug("桥接服务：跳过已缓存的 Index 验证 txID=%s", txID)
			}
		}
		if vcfg.VerifyReferenceChain {
			if cached, _ := s.indexStore.IsStepVerified(txID, index.StepRefChain); cached {
				runCfg.VerifyReferenceChain = false
				log.Debug("桥接服务：跳过已缓存的引用链验证 txID=%s", txID)
			}
		}
		if vcfg.VerifyIntegrity {
			if cached, _ := s.indexStore.IsStepVerified(txID, index.StepIntegrity); cached {
				runCfg.VerifyIntegrity = false
				log.Debug("桥接服务：跳过已缓存的完整性验证 txID=%s", txID)
			}
		}

		// 如果所有步骤都被跳过，创建一个全 passed 的结果（但保留已缓存的步骤结果）
		allSkipped := (!vcfg.VerifyPoW || runCfg.VerifyPoW == false) &&
			(!vcfg.VerifyIndex || runCfg.VerifyIndex == false) &&
			(!vcfg.VerifyReferenceChain || runCfg.VerifyReferenceChain == false) &&
			(!vcfg.VerifyIntegrity || runCfg.VerifyIntegrity == false)

		if allSkipped && (vcfg.VerifyPoW || vcfg.VerifyIndex || vcfg.VerifyReferenceChain || vcfg.VerifyIntegrity) {
			// 所有启用的步骤都已缓存，返回通过结果
			results := []pipeline.VerifyResult{
				{Step: pipeline.StepMetaValidate, Passed: true, Message: "cached"},
			}
			if vcfg.VerifyPoW {
				results = append(results, pipeline.VerifyResult{Step: pipeline.StepPoW, Passed: true, Message: "cached"})
			}
			if vcfg.VerifyIndex {
				results = append(results, pipeline.VerifyResult{Step: pipeline.StepIndexExistence, Passed: true, Message: "cached"})
				results = append(results, pipeline.VerifyResult{Step: pipeline.StepIndex, Passed: true, Message: "cached"})
			}
			if vcfg.VerifyReferenceChain {
				results = append(results, pipeline.VerifyResult{Step: pipeline.StepReferenceChain, Passed: true, Message: "cached"})
			}
			if vcfg.VerifyIntegrity {
				results = append(results, pipeline.VerifyResult{Step: pipeline.StepIntegrity, Passed: true, Message: "cached"})
			}
			return &pipeline.PipelineResult{Passed: true, Results: results}
		}

		// 创建临时 pipeline 使用修改后的配置
		p := pipeline.NewPipeline(runCfg)
		if s.bridge.GetGatewayClient() != nil {
			p.SetGatewayClient(s.bridge.GetGatewayClient())
		}
		// 注入 PoW 验证器（带日志）
		p.SetPoWVerifier(func(powStr, powAlg, rootCID, dataTXID string, dataSize int64) error {
			err := pow.Verify(powStr, powAlg, rootCID, dataTXID, dataSize)
			if err != nil {
				log.Warn("管道 PoW 验证失败：root_cid=%s, error=%v", rootCID, err)
			}
			return err
		})
		return p.Verify(meta, carAvailable)
	}

	// 没有索引存储，使用默认 bridge 运行
	return s.bridge.RunPipeline(meta, carAvailable)
}

// cacheStepResults 将管道步骤结果缓存到索引存储
func (s *Service) cacheStepResults(txID string, steps []pipeline.VerifyResult) {
	if s.indexStore == nil {
		return
	}

	for _, r := range steps {
		// 只缓存实际通过（非跳过）的步骤
		if r.Passed && !r.Skipped {
			if err := s.indexStore.MarkStepVerified(txID, r.Step); err != nil {
				log.Warn("桥接服务：缓存步骤验证失败 txID=%s step=%s: %v", txID, r.Step, err)
			} else {
				log.Debug("桥接服务：已缓存步骤验证 txID=%s step=%s", txID, r.Step)
			}
		}
	}
}

// strictVerifyMetadata 对指定 metaTxID 执行 strict 级别全步骤验证
//
// 实现 StrictVerifier 接口，供 BlockFetcher 在 Bitswap 请求时调用。
// 执行流程：
//  1. 获取元数据
//  2. 运行全开 pipeline（PoW + Index + RefChain + Integrity）
//  3. 将每个通过的步骤标记为已验证
//  4. 失败则返回错误
func (s *Service) strictVerifyMetadata(ctx context.Context, metaTxID string) error {
	log.Info("桥接服务：执行 strict 级别验证 metaTxID=%s", metaTxID)

	// 1. 获取元数据
	meta, err := s.fetcher.FetchMetadataByTXID(metaTxID)
	if err != nil {
		return fmt.Errorf("strict verify: fetch metadata %s: %w", metaTxID, err)
	}

	// 2. 运行全开 pipeline（PoW + Index + RefChain + Integrity）
	strictBridge := NewBridge(true, true, true, true)
	if s.bridge.GetGatewayClient() != nil {
		strictBridge.SetGatewayClient(s.bridge.GetGatewayClient())
	}
	quickResult := strictBridge.RunPipeline(meta, false)

	if !quickResult.Passed {
		// 记录失败详情
		for _, r := range quickResult.Results {
			if !r.Passed && !r.Skipped {
				log.Warn("桥接服务：strict 验证步骤 %s 失败: %s", r.Step, r.Error)
			}
		}
		return fmt.Errorf("strict verify: pipeline failed for %s", metaTxID)
	}

	// 3. 缓存每个通过的步骤
	for _, r := range quickResult.Results {
		if r.Passed && !r.Skipped {
			if err := s.indexStore.MarkStepVerified(metaTxID, r.Step); err != nil {
				log.Warn("桥接服务：缓存步骤验证失败 step=%s: %v", r.Step, err)
			}
		}
	}

	// 同时标记向后兼容的 verified 字段
	if err := s.indexStore.MarkVerified(metaTxID); err != nil {
		log.Warn("桥接服务：标记 verified 失败: %v", err)
	}

	log.Info("桥接服务：验证通过 metaTxID=%s", metaTxID)
	return nil
}

// GetOnlineVerifier 获取在线验证器
func (s *Service) GetOnlineVerifier() *verify.OnlineVerifier {
	return s.getOnlineVerifier()
}

// ============================================================
// Bitswap 按需拉取：CID 索引（使用两层索引架构）
// ============================================================

// indexMetadataCIDs 将元数据中的 CID 信息写入两层索引
func (s *Service) indexMetadataCIDs(metadataTxID string, meta *sdkmeta.Metadata) {
	if s.indexStore == nil {
		return
	}

	// 第二层：写入元数据索引
	bundleTXID := meta.BundleTXID
	if bundleTXID == "" {
		bundleTXID = "none"
	}
	params := map[string]string{
		"dataTxId":    meta.DataTXID,
		"bundleTxId":  bundleTXID,
		"blockHeight": fmt.Sprintf("%d", meta.DataHeight),
		"dataSize":    fmt.Sprintf("%d", meta.DataSize),
		"rootCid":     meta.RootCID,
		"verified":    "none",
		"verifiedAt":  "0",
	}
	if err := s.indexStore.IndexMeta(metadataTxID, params); err != nil {
		log.Warn("桥接服务：索引元数据失败 metaTxID=%s: %v", metadataTxID, err)
	} else {
		log.Debug("桥接服务：已索引元数据 metaTxID=%s", metadataTxID)
	}

	// 第一层：索引 RootCID → metaTxID
	if meta.RootCID != "" {
		log.Debug("两层索引：写入 CID=%s meta=%s", meta.RootCID, metadataTxID)
		if err := s.indexStore.IndexCID(meta.RootCID, metadataTxID); err != nil {
			log.Warn("桥接服务：索引 RootCID 失败 %s: %v", meta.RootCID, err)
		} else {
			log.Debug("桥接服务：已索引 CID %s → metaTxID=%s", meta.RootCID, metadataTxID)
		}
	}

	// 索引 Reference 中的 CID
	if meta.HasReference() {
		refMap := *meta.Reference
		for refTXID, entry := range refMap {
			for _, cidStr := range entry.CIDs {
				if err := s.indexStore.IndexCID(cidStr, refTXID); err != nil {
					log.Warn("桥接服务：索引引用 CID 失败 %s: %v", cidStr, err)
				} else {
					log.Debug("桥接服务：已索引引用 CID %s → tx=%s", cidStr, refTXID)
				}
			}
		}
	}

	log.Info("桥接服务：两层索引完成 root=%s meta=%s", meta.RootCID, metadataTxID)
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

// GetGraphQLScanner 获取 GraphQL 扫描器
func (s *Service) GetGraphQLScanner() *discovery.GraphQLScanner {
	return s.graphQLScanner
}

// GetBlockWatcher 获取区块监听器
func (s *Service) GetBlockWatcher() *discovery.BlockWatcher {
	return s.blockWatcher
}

// SetBadgerDB 注入外部 Badger DB
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
	Meta       *sdkmeta.Metadata       `json:"meta"`
	Passed     bool                    `json:"passed"`
	Incomplete bool                    `json:"incomplete,omitempty"` // 引用链不完整标记
	Steps      []pipeline.VerifyResult `json:"steps"`
	CARPath    string                  `json:"car_path,omitempty"`
	QuickOnly  bool                    `json:"quick_only,omitempty"`
	VerifyMode string                  `json:"verify_mode,omitempty"`
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

	if result.Incomplete {
		status += " ⚠️ 不完整"
	}

	modeLabel := ""
	switch result.VerifyMode {
	case "online":
		modeLabel = " (在线验证)"
	case "download":
		modeLabel = " (完整下载)"
	case "quick":
		modeLabel = " (仅快速验证)"
	case "cached":
		modeLabel = " (缓存命中)"
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
		} else if step.Incomplete {
			icon = "⚠️"
		}
		output += fmt.Sprintf("  %s %s", icon, step.Step)
		if step.Skipped {
			output += fmt.Sprintf(" (跳过: %s)", step.Message)
		} else if step.Incomplete {
			output += fmt.Sprintf(" (不完整: %s)", step.Message)
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
