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

	"github.com/lwdjd/IPFAR/internal/discovery"
	"github.com/lwdjd/IPFAR/internal/download"
	"github.com/lwdjd/IPFAR/internal/log"
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
}

// DefaultServiceConfig 返回默认服务配置
func DefaultServiceConfig() ServiceConfig {
	return ServiceConfig{
		Preset:                 pipeline.SecurityLight,
		MinBlockHeight:         0,
		MaxBlockHeight:         0, // 动态跟随
		PollInterval:           2 * time.Minute,
		CacheDir:               "cache/car",
		MaxFileSize:            200 * 1024 * 1024,
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

	// 在线验证器（延迟初始化）
	onlineVerifier     *verify.OnlineVerifier
	onlineVerifierOnce sync.Once

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

	log.Info("桥接服务：初始化完成 preset=%s verify_pow=%v verify_index=%v verify_ref=%v verify_integrity=%v online_verify=%v",
		cfg.Preset, verifyPoW, verifyIndex, verifyRef, verifyIntegrity, cfg.OnlineVerify)

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

// Start 启动桥接服务（阻塞）
func (s *Service) Start() error {
	log.Info("桥接服务：启动中...")
	log.Info("  安全预设: %s", s.config.Preset)
	log.Info("  缓存目录: %s", s.config.CacheDir)
	log.Info("  最大文件: %d bytes", s.config.MaxFileSize)
	log.Info("  在线验证: %v", s.config.OnlineVerify)
	log.Info("  下载并发: %d | 在线验证并发: %d",
		cap(s.downloadSema), cap(s.onlineSema))

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
	onlineResult, err := verifier.Verify(meta.DataTXID)
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

// GetOnlineVerifier 获取在线验证器
func (s *Service) GetOnlineVerifier() *verify.OnlineVerifier {
	return s.getOnlineVerifier()
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
