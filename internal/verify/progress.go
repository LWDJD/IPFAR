// Package verify 桥节点验证进度回调
// 保留桥 UI 进度回调作为中间件注入到 SDK 管道
package verify

import (
	"context"

	sdkmeta "github.com/LWDJD/ipfar-sdk/verify/metadata"
	"github.com/LWDJD/ipfar-sdk/verify/pipeline"
)

// ProgressMiddleware 进度中间件
// 在 SDK 管道执行过程中注入桥 UI 进度回调
type ProgressMiddleware struct {
	callback VerifyProgressCallback
}

// NewProgressMiddleware 创建进度中间件
func NewProgressMiddleware(cb VerifyProgressCallback) *ProgressMiddleware {
	return &ProgressMiddleware{
		callback: cb,
	}
}

// WrapPipeline 将进度回调注入到 Pipeline 中
// 为每个验证步骤注入带进度通知的包装器
func (pm *ProgressMiddleware) WrapPipeline(p *pipeline.Pipeline) {
	if pm.callback == nil {
		return
	}

	// 注入元数据验证的进度回调
	p.SetMetaValidator(func(meta *sdkmeta.Metadata) error {
		pm.callback(pipeline.StepMetaValidate, 0.05, "验证元数据合法性...")
		err := meta.Validate()
		if err != nil {
			pm.callback(pipeline.StepMetaValidate, 0.1, "元数据验证失败")
			return err
		}
		pm.callback(pipeline.StepMetaValidate, 0.1, "元数据验证通过")
		return nil
	})

	// 注入 PoW 验证的进度回调
	p.SetPoWVerifier(func(powStr, powAlg, rootCID, dataTXID string, dataSize int64) error {
		pm.callback(pipeline.StepPoW, 0.15, "验证 PoW...")
		err := VerifyPoW(powStr, powAlg, rootCID, dataTXID, dataSize)
		if err != nil {
			pm.callback(pipeline.StepPoW, 0.3, "PoW 验证失败")
			return err
		}
		pm.callback(pipeline.StepPoW, 0.3, "PoW 验证通过")
		return nil
	})
}

// NotifyProgress 发送进度通知（便捷函数）
func NotifyProgress(cb VerifyProgressCallback, step string, progress float64, message string) {
	if cb != nil {
		cb(step, progress, message)
	}
}

// NopProgressCallback 空进度回调（不做任何事）
func NopProgressCallback(step string, progress float64, message string) {}

// contextKey 用于在 context 中存储进度回调
type contextKey string

const progressCallbackKey contextKey = "verify_progress_callback"

// WithProgressCallback 将进度回调注入到 context 中
func WithProgressCallback(ctx context.Context, cb VerifyProgressCallback) context.Context {
	return context.WithValue(ctx, progressCallbackKey, cb)
}

// GetProgressCallback 从 context 中提取进度回调
func GetProgressCallback(ctx context.Context) VerifyProgressCallback {
	if cb, ok := ctx.Value(progressCallbackKey).(VerifyProgressCallback); ok {
		return cb
	}
	return nil
}
