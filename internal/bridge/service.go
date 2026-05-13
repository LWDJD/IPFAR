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
	CarAvailable bool // 是否在验证前下载 CAR（默认 true）
}

// DefaultServiceConfig 返回默认服务配置
func DefaultServiceConfig() ServiceConfig {
	return ServiceConfig{
		Preset:                pipeline.SecurityLight,
		MinBlockHeight:        0,
		MaxBlockHeight:        0, // 动态跟随
		PollInterval:          2 * time.Minute,
		CacheDir:              "cache/car",
		MaxFileSize:           0, // 不限制
		CarAvailable:          true,
	}
}

// Service 桥接服务，集成发现 → 下载 → 验证完整链路
type Service struct {
	config  ServiceConfig
	bridge  *Bridge
	fetcher *download.Fetcher
	sampler *discovery.Sampler

	// 上下文
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// 统计
	mu       sync.RWMutex
	stats    ServiceStats
}

// ServiceStats 服务统计
type ServiceStats struct {
	BlocksChecked   uint64 `json:"blocks_checked"`
	BlocksFound     uint64 `json:"blocks_found"`
	MetadataFetched uint64 `json:"metadata_fetched"`
	CARsDownloaded  uint64 `json:"cars_downloaded"`
	PipelinesPassed uint64 `json:"pipelines_passed"`
	PipelinesFailed uint64 `json:"pipelines_failed"`
	StartedAt       time.Time `json:"started_at"`
	LastActivity    time.Time `json:"last_activity"`
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

	ctx, cancel := context.WithCancel(context.Background())

	svc := &Service{
		config:  cfg,
		bridge:  bridge,
		fetcher: fetcher,
		ctx:     ctx,
		cancel:  cancel,
		stats: ServiceStats{
			StartedAt: time.Now(),
		},
	}

	log.Info("桥接服务：初始化完成 preset=%s verify_pow=%v verify_index=%v verify_ref=%v verify_integrity=%v",
		cfg.Preset, verifyPoW, verifyIndex, verifyRef, verifyIntegrity)

	return svc, nil
}

// Start 启动桥接服务（阻塞）
func (s *Service) Start() error {
	log.Info("桥接服务：启动中...")
	log.Info("  安全预设: %s", s.config.Preset)
	log.Info("  缓存目录: %s", s.config.CacheDir)
	log.Info("  最大文件: %d bytes", s.config.MaxFileSize)

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
// 完整链路：获取元数据 → 下载 CAR → 运行验证管道
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
		}, nil
	}

	// Step 3: 下载 CAR 文件（如果配置要求）
	var carPath string
	if s.config.CarAvailable {
		carPath, err = s.fetcher.DownloadCAR(meta)
		if err != nil {
			log.Warn("桥接服务：CAR 下载失败 root_cid=%s: %v", meta.RootCID, err)
			// 继续用快速验证结果
			return &PipelineResult{
				Meta:      meta,
				Passed:    quickResult.Passed,
				Steps:     quickResult.Results,
				CARPath:   "",
				QuickOnly: true,
			}, nil
		}

		s.mu.Lock()
		s.stats.CARsDownloaded++
		s.mu.Unlock()

		log.Info("桥接服务：CAR 文件已下载 root_cid=%s path=%s", meta.RootCID, carPath)
	} else {
		// 不下载 CAR，仅返回快速验证结果
		return &PipelineResult{
			Meta:      meta,
			Passed:    quickResult.Passed,
			Steps:     quickResult.Results,
			CARPath:   "",
			QuickOnly: true,
		}, nil
	}

	// Step 4: 运行完整验证管道
	fullResult := s.bridge.RunPipeline(meta, carPath != "")

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
		Meta:   meta,
		Passed: fullResult.Passed,
		Steps:  fullResult.Results,
		CARPath: carPath,
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
			Meta:      meta,
			Passed:    false,
			Steps:     quickResult.Results,
			QuickOnly: true,
		}, nil
	}

	// 下载 CAR 并完整验证
	var carPath string
	var err error
	if s.config.CarAvailable {
		carPath, err = s.fetcher.DownloadCAR(meta)
		if err != nil {
			log.Warn("桥接服务：CAR 下载失败: %v", err)
		} else {
			s.mu.Lock()
			s.stats.CARsDownloaded++
			s.mu.Unlock()
		}
	}

	fullResult := s.bridge.RunPipeline(meta, carPath != "")

	if fullResult.Passed {
		s.mu.Lock()
		s.stats.PipelinesPassed++
		s.mu.Unlock()
	} else {
		s.mu.Lock()
		s.stats.PipelinesFailed++
		s.mu.Unlock()
	}

	return &PipelineResult{
		Meta:    meta,
		Passed:  fullResult.Passed,
		Steps:   fullResult.Results,
		CARPath: carPath,
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

// ============================================================
// 类型定义
// ============================================================

// PipelineResult 管道处理结果（扩展版）
type PipelineResult struct {
	Meta      *sdkmeta.Metadata            `json:"meta"`
	Passed    bool                         `json:"passed"`
	Steps     []pipeline.VerifyResult      `json:"steps"`
	CARPath   string                       `json:"car_path,omitempty"`
	QuickOnly bool                         `json:"quick_only,omitempty"`
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
	if result.QuickOnly {
		status += " (仅快速验证)"
	}

	output := fmt.Sprintf("验证结果: %s\n", status)
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
		}
		output += "\n"
	}

	if result.CARPath != "" {
		output += fmt.Sprintf("\n  CAR 缓存: %s\n", result.CARPath)
	}

	output += "──────────────────────────────────────\n"
	return output
}
