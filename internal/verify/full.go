// Package verify 提供全量验证功能，对已缓存的所有 CAR 文件执行完整的重新验证。
//
// 全量验证 = 在 strict 预设基础上，遍历缓存中的所有 CAR 文件，
// 重新执行完整验证链（PoW → Index → ReferenceChain → Integrity）。
//
// 触发方式：CLI 命令 `ipfar verify full`
//
// 报告验证结果：通过/失败/跳过
package verify

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	sdkmeta "github.com/LWDJD/ipfar-sdk/verify/metadata"
	"github.com/LWDJD/ipfar-sdk/verify/pipeline"
	"github.com/LWDJD/ipfar-sdk/verify/pow"

	"github.com/lwdjd/IPFAR/internal/download"
	"github.com/lwdjd/IPFAR/internal/log"
)

// FullVerifyResult 单个 CAR 文件的全量验证结果
type FullVerifyResult struct {
	// RootCID 根 CID
	RootCID string `json:"root_cid"`
	// DataTXID 数据交易 ID
	DataTXID string `json:"data_txid"`
	// CARPath CAR 文件路径
	CARPath string `json:"car_path"`
	// FileSize CAR 文件大小（字节）
	FileSize int64 `json:"file_size"`
	// Passed 验证是否通过
	Passed bool `json:"passed"`
	// Skipped 是否被跳过（如文件损坏、元数据不可用等）
	Skipped bool `json:"skipped"`
	// Error 错误信息（如果有）
	Error string `json:"error,omitempty"`
	// Steps 各验证步骤结果
	Steps []pipeline.VerifyResult `json:"steps,omitempty"`
	// Duration 验证耗时
	Duration time.Duration `json:"duration"`
}

// FullVerifyReport 全量验证汇总报告
type FullVerifyReport struct {
	// TotalFiles 扫描到的总文件数
	TotalFiles int `json:"total_files"`
	// VerifiedFiles 实际验证的文件数
	VerifiedFiles int `json:"verified_files"`
	// PassedFiles 验证通过的文件数
	PassedFiles int `json:"passed_files"`
	// FailedFiles 验证失败的文件数
	FailedFiles int `json:"failed_files"`
	// SkippedFiles 跳过的文件数
	SkippedFiles int `json:"skipped_files"`
	// Results 每个文件的详细结果
	Results []FullVerifyResult `json:"results"`
	// TotalDuration 总耗时
	TotalDuration time.Duration `json:"total_duration"`
	// StartedAt 开始时间
	StartedAt time.Time `json:"started_at"`
	// FinishedAt 结束时间
	FinishedAt time.Time `json:"finished_at"`
}

// FullVerifierConfig 全量验证器配置
type FullVerifierConfig struct {
	// CacheDir CAR 文件缓存目录（与 Fetcher 的 CacheDir 保持一致）
	CacheDir string
	// GatewayURLs 网关 URL 列表（用于重新获取元数据）
	GatewayURLs []string
	// MaxConcurrency 最大并发验证数（默认 2）
	MaxConcurrency int
	// VerifyPoW 是否验证 PoW（全量模式默认 true）
	VerifyPoW bool
	// VerifyIndex 是否验证索引（全量模式默认 true）
	VerifyIndex bool
	// VerifyReferenceChain 是否验证引用链（全量模式默认 true）
	VerifyReferenceChain bool
	// VerifyIntegrity 是否验证完整性（全量模式默认 true）
	VerifyIntegrity bool
	// SkipMissingMeta 元数据不可获取时是否跳过（默认 true）
	SkipMissingMeta bool
}

// DefaultFullVerifierConfig 返回默认全量验证器配置
func DefaultFullVerifierConfig() FullVerifierConfig {
	return FullVerifierConfig{
		CacheDir:              "cache/car",
		MaxConcurrency:        2,
		VerifyPoW:             true,
		VerifyIndex:           true,
		VerifyReferenceChain:  true,
		VerifyIntegrity:       true,
		SkipMissingMeta:       true,
	}
}

// FullVerifier 全量验证器
// 遍历缓存中的所有 CAR 文件，重新执行完整验证链
type FullVerifier struct {
	config  FullVerifierConfig
	gateway *download.Gateway
	sema    chan struct{} // 并发控制信号量

	// 管道配置
	pipelineConfig pipeline.VerifyConfig
}

// NewFullVerifier 创建新的全量验证器
func NewFullVerifier(config FullVerifierConfig) *FullVerifier {
	if config.MaxConcurrency <= 0 {
		config.MaxConcurrency = 2
	}
	if config.CacheDir == "" {
		config.CacheDir = "cache/car"
	}

	gatewayURLs := config.GatewayURLs
	if len(gatewayURLs) == 0 {
		gatewayURLs = []string{"https://arweave.net"}
	}

	gw := download.NewGateway(download.GatewayConfig{
		URLs:       gatewayURLs,
		Timeout:    30 * time.Second,
		MaxRetries: 3,
		RetryDelay: 1 * time.Second,
	})

	pipeConfig := pipeline.VerifyConfig{
		VerifyPoW:             config.VerifyPoW,
		VerifyIndex:           config.VerifyIndex,
		VerifyReferenceChain:  config.VerifyReferenceChain,
		VerifyIntegrity:       config.VerifyIntegrity,
	}

	return &FullVerifier{
		config:         config,
		gateway:        gw,
		sema:           make(chan struct{}, config.MaxConcurrency),
		pipelineConfig: pipeConfig,
	}
}

// GetPipelineConfig 获取管道配置（用于外部检查）
func (fv *FullVerifier) GetPipelineConfig() pipeline.VerifyConfig {
	return fv.pipelineConfig
}

// VerifyAll 执行全量验证
// 扫描缓存目录中所有 CAR 文件，逐一重新验证
func (fv *FullVerifier) VerifyAll() (*FullVerifyReport, error) {
	report := &FullVerifyReport{
		StartedAt: time.Now(),
	}

	log.Info("全量验证：开始扫描缓存目录 %s", fv.config.CacheDir)

	// Step 1: 扫描 CAR 文件
	carFiles, err := fv.scanCarFiles()
	if err != nil {
		return nil, fmt.Errorf("扫描 CAR 文件失败: %w", err)
	}

	report.TotalFiles = len(carFiles)
	log.Info("全量验证：发现 %d 个 CAR 文件", report.TotalFiles)

	if len(carFiles) == 0 {
		report.FinishedAt = time.Now()
		report.TotalDuration = report.FinishedAt.Sub(report.StartedAt)
		return report, nil
	}

	// Step 2: 并发验证每个文件
	type verifyOutcome struct {
		index  int
		result FullVerifyResult
	}

	var wg sync.WaitGroup
	resultsCh := make(chan verifyOutcome, len(carFiles))

	for i, carFile := range carFiles {
		wg.Add(1)
		go func(idx int, cf carFileInfo) {
			defer wg.Done()

			// 获取并发信号量
			fv.sema <- struct{}{}
			defer func() { <-fv.sema }()

			result := fv.verifyOne(cf)
			resultsCh <- verifyOutcome{index: idx, result: result}
		}(i, carFile)
	}

	wg.Wait()
	close(resultsCh)

	// Step 3: 收集结果
	report.Results = make([]FullVerifyResult, len(carFiles))
	for outcome := range resultsCh {
		report.Results[outcome.index] = outcome.result
	}

	// Step 4: 汇总统计
	for _, r := range report.Results {
		if r.Skipped {
			report.SkippedFiles++
		} else {
			report.VerifiedFiles++
			if r.Passed {
				report.PassedFiles++
			} else {
				report.FailedFiles++
			}
		}
	}

	report.FinishedAt = time.Now()
	report.TotalDuration = report.FinishedAt.Sub(report.StartedAt)

	log.Info("全量验证：完成 total=%d verified=%d passed=%d failed=%d skipped=%d duration=%s",
		report.TotalFiles, report.VerifiedFiles, report.PassedFiles,
		report.FailedFiles, report.SkippedFiles, report.TotalDuration)

	return report, nil
}

// carFileInfo 扫描到的 CAR 文件信息
type carFileInfo struct {
	Path     string // CAR 文件完整路径
	FileName string // 文件名
	Size     int64  // 文件大小
}

// scanCarFiles 扫描缓存目录下的所有 .car 文件
func (fv *FullVerifier) scanCarFiles() ([]carFileInfo, error) {
	var files []carFileInfo

	err := filepath.Walk(fv.config.CacheDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			// 权限错误等，跳过
			log.Debug("全量验证：扫描跳过 %s: %v", path, err)
			return nil
		}

		if info.IsDir() {
			return nil
		}

		if strings.HasSuffix(strings.ToLower(info.Name()), ".car") {
			files = append(files, carFileInfo{
				Path:     path,
				FileName: info.Name(),
				Size:     info.Size(),
			})
		}

		return nil
	})

	return files, err
}

// verifyOne 验证单个 CAR 文件
func (fv *FullVerifier) verifyOne(cf carFileInfo) FullVerifyResult {
	start := time.Now()

	result := FullVerifyResult{
		CARPath:  cf.Path,
		FileSize: cf.Size,
	}

	log.Info("全量验证：处理 %s (%d bytes)", cf.FileName, cf.Size)

	// Step 1: 从文件名解析 root_cid 和 data_txid
	rootCID, dataTXID := parseCarFileName(cf.FileName)
	result.RootCID = rootCID
	result.DataTXID = dataTXID

	if rootCID == "" {
		result.Skipped = true
		result.Error = "无法从文件名解析 root_cid"
		result.Duration = time.Since(start)
		log.Warn("全量验证：跳过 %s: 无法解析 root_cid", cf.FileName)
		return result
	}

	// Step 2: 重新获取元数据（从 Arweave 网关）
	var meta *sdkmeta.Metadata
	var metaErr error

	if dataTXID != "" {
		// 尝试从 Arweave 获取元数据交易
		meta, metaErr = fv.fetchMetadata(dataTXID)
	}

	if metaErr != nil || meta == nil {
		if fv.config.SkipMissingMeta {
			result.Skipped = true
			if metaErr != nil {
				result.Error = fmt.Sprintf("无法获取元数据: %v", metaErr)
			} else {
				result.Error = "元数据为空"
			}
			result.Duration = time.Since(start)
			log.Warn("全量验证：跳过 %s: %s", cf.FileName, result.Error)
			return result
		}
		result.Passed = false
		result.Error = fmt.Sprintf("无法获取元数据: %v", metaErr)
		result.Duration = time.Since(start)
		return result
	}

	// Step 3: 运行完整验证管道（carAvailable=true，使用本地 CAR 文件）
	pipeResult := fv.runPipeline(meta, true)
	result.Steps = pipeResult.Results
	result.Passed = pipeResult.Passed
	result.Duration = time.Since(start)

	if result.Passed {
		log.Info("全量验证：通过 ✅ %s root_cid=%s duration=%s",
			cf.FileName, rootCID, result.Duration)
	} else {
		log.Warn("全量验证：失败 ❌ %s root_cid=%s duration=%s",
			cf.FileName, rootCID, result.Duration)
	}

	return result
}

// runPipeline 运行验证管道
// 直接使用 SDK pipeline，避免循环导入 bridge 包
func (fv *FullVerifier) runPipeline(meta *sdkmeta.Metadata, carAvailable bool) *pipeline.PipelineResult {
	p := pipeline.NewPipeline(fv.pipelineConfig)

	// 注入带有日志的 PoW 验证器
	p.SetPoWVerifier(func(powStr, powAlg, rootCID, dataTXID string, dataSize int64) error {
		err := pow.Verify(powStr, powAlg, rootCID, dataTXID, dataSize)
		if err != nil {
			log.Warn("全量验证 PoW 失败：root_cid=%s, error=%v", rootCID, err)
		}
		return err
	})

	result := p.Verify(meta, carAvailable)

	// 记录结果
	if result.Passed {
		log.Info("全量验证管道通过：root_cid=%s, steps=%d", meta.RootCID, len(result.Results))
	} else {
		log.Warn("全量验证管道失败：root_cid=%s", meta.RootCID)
		for _, r := range result.Results {
			if !r.Passed && !r.Skipped {
				log.Warn("  - %s: %s", r.Step, r.Error)
			}
		}
	}

	return result
}

// fetchMetadata 从 Arweave 网关获取元数据交易
func (fv *FullVerifier) fetchMetadata(txID string) (*sdkmeta.Metadata, error) {
	data, err := fv.gateway.FetchTransaction(txID)
	if err != nil {
		return nil, fmt.Errorf("获取元数据交易 %s 失败: %w", txID, err)
	}

	// 先尝试 JSON 直接解析
	meta, err := sdkmeta.ParseAndValidate(data)
	if err != nil {
		// 尝试 Base64URL 解码
		meta, err = sdkmeta.ParseAndValidateBase64URL(string(data))
		if err != nil {
			return nil, fmt.Errorf("解析元数据失败: %w", err)
		}
	}

	return meta, nil
}

// parseCarFileName 从 CAR 文件名中提取 root_cid 和 data_txid
//
// 文件名格式（由 Fetcher.cachePath 生成）：
//
//	{root_cid[:32]}_{data_txid[:12]}.car
//
// 返回 (rootCID, dataTXID)。如果无法解析返回空字符串。
func parseCarFileName(fileName string) (string, string) {
	// 去除 .car 后缀
	name := strings.TrimSuffix(fileName, ".car")
	name = strings.TrimSuffix(name, ".CAR")

	// 寻找最后一个下划线分隔 data_txid
	// 格式: {root_cid}_{data_txid}
	// root_cid 可能包含大小写字母数字，data_txid 也是
	// 由于 root_cid 被截断到 32 字符，data_txid 被截断到 12 字符

	// 使用文件名中最后一个下划线作为分隔
	lastUnderscore := strings.LastIndex(name, "_")
	if lastUnderscore <= 0 || lastUnderscore >= len(name)-1 {
		return "", ""
	}

	dataTXIDPart := name[lastUnderscore+1:]
	rootCIDPart := name[:lastUnderscore]

	// data_txid 部分至少 1 字符
	if len(dataTXIDPart) < 1 {
		return "", ""
	}

	// root_cid 部分至少 1 字符
	if len(rootCIDPart) < 1 {
		return "", ""
	}

	return rootCIDPart, dataTXIDPart
}

// ============================================================
// 报告格式化
// ============================================================

// FormatReport 格式化全量验证报告为可读字符串
func FormatReport(report *FullVerifyReport) string {
	if report == nil {
		return "无验证报告"
	}

	var sb strings.Builder

	sb.WriteString("\n")
	sb.WriteString("╔══════════════════════════════════════╗\n")
	sb.WriteString("║       全量验证报告                   ║\n")
	sb.WriteString("╠══════════════════════════════════════╣\n")

	fmt.Fprintf(&sb, "║ 扫描文件数:    %-22d ║\n", report.TotalFiles)
	fmt.Fprintf(&sb, "║ 已验证:        %-22d ║\n", report.VerifiedFiles)
	fmt.Fprintf(&sb, "║ 通过:          %-22d ║\n", report.PassedFiles)
	fmt.Fprintf(&sb, "║ 失败:          %-22d ║\n", report.FailedFiles)
	fmt.Fprintf(&sb, "║ 跳过:          %-22d ║\n", report.SkippedFiles)
	fmt.Fprintf(&sb, "║ 总耗时:        %-22s ║\n", report.TotalDuration.Round(time.Millisecond))
	sb.WriteString("╚══════════════════════════════════════╝\n")

	if len(report.Results) > 0 {
		sb.WriteString("\n详细结果:\n")
		sb.WriteString("──────────────────────────────────────\n")

		for i, r := range report.Results {
			icon := "✅"
			status := "通过"
			if r.Skipped {
				icon = "⏭️"
				status = "跳过"
			} else if !r.Passed {
				icon = "❌"
				status = "失败"
			}

			fmt.Fprintf(&sb, "%s [%d] %s\n", icon, i+1, r.CARPath)
			fmt.Fprintf(&sb, "   状态: %s | 耗时: %s\n", status, r.Duration.Round(time.Millisecond))

			if r.RootCID != "" {
				fmt.Fprintf(&sb, "   Root CID: %s\n", r.RootCID)
			}
			if r.DataTXID != "" {
				fmt.Fprintf(&sb, "   Data TXID: %s\n", r.DataTXID)
			}
			if r.Error != "" {
				fmt.Fprintf(&sb, "   错误: %s\n", r.Error)
			}

			// 显示各步骤
			for _, step := range r.Steps {
				stepIcon := "  ✅"
				if !step.Passed && !step.Skipped {
					stepIcon = "  ❌"
				} else if step.Skipped {
					stepIcon = "  ⏭️"
				}
				sb.WriteString(fmt.Sprintf("%s %s", stepIcon, step.Step))
				if step.Skipped {
					sb.WriteString(fmt.Sprintf(" (跳过: %s)", step.Message))
				} else if !step.Passed {
					sb.WriteString(fmt.Sprintf(" (失败: %s)", step.Error))
				} else if step.Message != "" {
					sb.WriteString(fmt.Sprintf(" (%s)", step.Message))
				}
				sb.WriteString("\n")
			}
			sb.WriteString("\n")
		}
		sb.WriteString("──────────────────────────────────────\n")
	}

	return sb.String()
}

// HasFailures 检查是否有验证失败
func (r *FullVerifyReport) HasFailures() bool {
	return r.FailedFiles > 0
}

// AllPassedOrSkipped 是否全部通过或跳过（没有失败）
func (r *FullVerifyReport) AllPassedOrSkipped() bool {
	return r.FailedFiles == 0
}
