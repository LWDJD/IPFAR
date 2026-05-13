// Package bridge 提供 IPFAR 桥接核心逻辑，连接 SDK 模块与主程序
// 规范参考: ipfar-specs/V1/项目规划.md
package bridge

import (
	"encoding/json"
	"fmt"

	"github.com/LWDJD/ipfar-sdk/verify/metadata"
	"github.com/LWDJD/ipfar-sdk/verify/pipeline"
	"github.com/LWDJD/ipfar-sdk/verify/pow"

	"github.com/lwdjd/IPFAR/internal/log"
)

// Bridge 桥接器，封装核心验证和工作流
type Bridge struct {
	config pipeline.VerifyConfig
}

// NewBridge 创建新的桥接器
func NewBridge(verifyPoW, verifyIndex, verifyRef, verifyIntegrity bool) *Bridge {
	return &Bridge{
		config: pipeline.VerifyConfig{
			VerifyPoW:            verifyPoW,
			VerifyIndex:          verifyIndex,
			VerifyReferenceChain: verifyRef,
			VerifyIntegrity:      verifyIntegrity,
		},
	}
}

// NewBridgeFromPreset 从安全预设创建桥接器
func NewBridgeFromPreset(preset string) (*Bridge, error) {
	config, err := pipeline.GetPreset(preset)
	if err != nil {
		return nil, err
	}
	return &Bridge{config: config}, nil
}

// VerifyMetadata 验证元数据 JSON
// 解析并验证元数据的合法性（必填字段、类型、条件字段等）
func (b *Bridge) VerifyMetadata(jsonData []byte) (*metadata.Metadata, error) {
	meta, err := metadata.ParseAndValidate(jsonData)
	if err != nil {
		log.Warn("元数据验证失败：%v", err)
		return nil, fmt.Errorf("metadata validation failed: %w", err)
	}

	log.Info("元数据验证通过：root_cid=%s, data_txid=%s, data_size=%d",
		meta.RootCID, meta.DataTXID, meta.DataSize)

	return meta, nil
}

// VerifyMetadataFromBase64 从 Base64URL 编码的字符串验证元数据
func (b *Bridge) VerifyMetadataFromBase64(encoded string) (*metadata.Metadata, error) {
	meta, err := metadata.ParseAndValidateBase64URL(encoded)
	if err != nil {
		log.Warn("元数据验证失败（Base64URL）：%v", err)
		return nil, fmt.Errorf("metadata validation failed: %w", err)
	}

	log.Info("元数据验证通过（Base64URL）：root_cid=%s", meta.RootCID)

	return meta, nil
}

// VerifyPoW 验证工作量证明
// 如果文件 >= 100 MiB，自动跳过
func (b *Bridge) VerifyPoW(meta *metadata.Metadata) error {
	if meta == nil {
		return fmt.Errorf("metadata is nil")
	}

	err := pow.Verify(meta.PoW, meta.PoWAlg, meta.RootCID, meta.DataTXID, int64(meta.DataSize))
	if err != nil {
		log.Warn("PoW 验证失败：root_cid=%s, data_size=%d, error=%v",
			meta.RootCID, meta.DataSize, err)
		return fmt.Errorf("PoW verification failed: %w", err)
	}

	if meta.NeedsPoW() {
		log.Info("PoW 验证通过：root_cid=%s, pow=%s", meta.RootCID, meta.PoW)
	} else {
		log.Info("文件大小 %d >= 100 MiB，免 PoW 验证", meta.DataSize)
	}

	return nil
}

// RunPipeline 运行完整验证管道
// carAvailable 表示 CAR 文件是否已经下载可用
func (b *Bridge) RunPipeline(meta *metadata.Metadata, carAvailable bool) *pipeline.PipelineResult {
	p := pipeline.NewPipeline(b.config)

	// 注入日志记录
	p.SetPoWVerifier(func(powStr, powAlg, rootCID, dataTXID string, dataSize int64) error {
		err := pow.Verify(powStr, powAlg, rootCID, dataTXID, dataSize)
		if err != nil {
			log.Warn("管道 PoW 验证失败：root_cid=%s, error=%v", rootCID, err)
		}
		return err
	})

	result := p.Verify(meta, carAvailable)

	// 记录结果
	if result.Passed {
		log.Info("验证管道通过：root_cid=%s, steps=%d", meta.RootCID, len(result.Results))
	} else {
		log.Warn("验证管道失败：root_cid=%s", meta.RootCID)
		for _, r := range result.Results {
			if !r.Passed && !r.Skipped {
				log.Warn("  - %s: %s", r.Step, r.Error)
			}
		}
	}

	return result
}

// PrintPipelineResult 格式化输出管道验证结果
func PrintPipelineResult(result *pipeline.PipelineResult) string {
	if result == nil {
		return "无验证结果"
	}

	status := "通过"
	if !result.Passed {
		status = "失败"
	}

	output := fmt.Sprintf("验证管道结果：%s\n", status)
	output += "----------------------------------------\n"

	for _, r := range result.Results {
		icon := "✅"
		if !r.Passed && !r.Skipped {
			icon = "❌"
		} else if r.Skipped {
			icon = "⏭️"
		}

		output += fmt.Sprintf("%s %s", icon, r.Step)

		if r.Skipped {
			output += fmt.Sprintf(" (跳过: %s)", r.Message)
		} else if !r.Passed {
			output += fmt.Sprintf(" (失败: %s)", r.Error)
		}

		output += "\n"
	}

	output += "----------------------------------------\n"

	return output
}

// FormatMetadataJSON 格式化元数据为可读的 JSON 字符串
func FormatMetadataJSON(meta *metadata.Metadata) (string, error) {
	if meta == nil {
		return "", fmt.Errorf("metadata is nil")
	}

	jsonBytes, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return "", err
	}

	return string(jsonBytes), nil
}

// GetConfig 获取当前桥接器的验证配置
func (b *Bridge) GetConfig() pipeline.VerifyConfig {
	return b.config
}

// PoWInfo 返回 PoW 相关常量信息
type PoWInfo struct {
	Algorithm           string `json:"algorithm"`
	Memory              string `json:"memory"`
	MinLeadingZeroBytes int    `json:"min_leading_zero_bytes"`
	Threshold           string `json:"threshold"`
}

// GetPoWInfo 获取 PoW 参数信息
func GetPoWInfo() PoWInfo {
	return PoWInfo{
		Algorithm:           pow.Algorithm,
		Memory:              "20 MB",
		MinLeadingZeroBytes: pow.MinLeadingZeroBytes,
		Threshold:           "100 MiB",
	}
}
