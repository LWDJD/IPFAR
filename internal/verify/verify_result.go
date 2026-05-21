// Package verify 桥节点验证类型定义
// 提供桥层面的 VerifyResult / VerifyInput / VerifyProgressCallback，
// 内部转译为 SDK pipeline 类型。
package verify

import (
	"context"

	sdkmeta "github.com/LWDJD/ipfar-sdk/verify/metadata"
	"github.com/LWDJD/ipfar-sdk/verify/pipeline"
)

// VerifyResult 桥节点验证结果
// 保持向后兼容：字段名/类型与 pipeline.VerifyResult 对齐
type VerifyResult struct {
	// Passed 验证是否全部通过
	Passed bool `json:"passed"`
	// Steps 各验证步骤结果
	Steps []pipeline.VerifyResult `json:"steps"`
	// Meta 已验证的元数据（可选）
	Meta *sdkmeta.Metadata `json:"meta,omitempty"`
	// Error 顶级错误信息（如果有）
	Error string `json:"error,omitempty"`
}

// VerifyInput 桥节点验证输入
// 桥层面结构体，内部转译为 SDK pipeline 调用
type VerifyInput struct {
	// Meta 已解析的元数据（必填）
	Meta *sdkmeta.Metadata
	// CARPath CAR 文件本地路径（可选，若为空则仅执行元数据+PoW验证）
	CARPath string
	// Preset 安全预设名称: strict, balanced, light, trusted
	Preset string
	// Config 验证配置（覆盖 Preset）
	Config *pipeline.VerifyConfig
	// Callback 进度回调（可选）
	Callback VerifyProgressCallback
}

// VerifyProgressCallback 验证进度回调函数类型
// step: 当前步骤标识 (meta_validate, pow, index, reference_chain, integrity)
// progress: 0.0 ~ 1.0 表示进度
// message: 步骤描述信息
type VerifyProgressCallback func(step string, progress float64, message string)

// NewVerifyResult 从 SDK pipeline 结果创建桥节点 VerifyResult
func NewVerifyResult(pipeResult *pipeline.PipelineResult, meta *sdkmeta.Metadata) *VerifyResult {
	if pipeResult == nil {
		return &VerifyResult{
			Passed: false,
			Error:  "nil pipeline result",
		}
	}
	return &VerifyResult{
		Passed: pipeResult.Passed,
		Steps:  pipeResult.Results,
		Meta:   meta,
	}
}

// NewVerifyResultError 创建错误结果
func NewVerifyResultError(err error) *VerifyResult {
	return &VerifyResult{
		Passed: false,
		Error:  err.Error(),
	}
}

// Verify 使用 FullVerifier 执行单次验证
// 便捷函数：创建 FullVerifier 并调用其 Verify 方法
func Verify(ctx context.Context, input *VerifyInput) (*VerifyResult, error) {
	v := NewFullVerifier(FullVerifierConfig{
		VerifyPoW:             input.Config != nil && input.Config.VerifyPoW,
		VerifyIndex:           input.Config != nil && input.Config.VerifyIndex,
		VerifyReferenceChain:  input.Config != nil && input.Config.VerifyReferenceChain,
		VerifyIntegrity:       input.Config != nil && input.Config.VerifyIntegrity,
	})
	return v.Verify(ctx, input)
}
