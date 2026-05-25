// Package discovery 提供 IPFAR 数据发现功能
//
// LightVerifier 轻量验证器：只做元数据格式校验 + PoW 验证，
// 不做完整的 CAR 文件 pipeline 验证。
// 用于 GraphQL 扫描器快速过滤无效元数据。
package discovery

import (
	"fmt"

	sdkmeta "github.com/LWDJD/ipfar-sdk/verify/metadata"
	sdkpow "github.com/LWDJD/ipfar-sdk/verify/pow"

	"github.com/lwdjd/IPFAR/internal/log"
)

// LightVerifier 轻量验证器
// 只做元数据校验 + PoW 验证，不做完整 pipeline
type LightVerifier struct {
	config LightVerifierConfig
}

// LightVerifierConfig 轻量验证器配置
type LightVerifierConfig struct {
	// SkipPoW 是否跳过 PoW 验证（默认 false）
	SkipPoW bool
	// MaxPoWTime PoW 最大验证时间（0 使用默认 10s）
	MaxPoWTime int
}

// DefaultLightVerifierConfig 返回默认配置
func DefaultLightVerifierConfig() LightVerifierConfig {
	return LightVerifierConfig{
		SkipPoW:    false,
		MaxPoWTime: 10,
	}
}

// NewLightVerifier 创建轻量验证器
func NewLightVerifier(cfg LightVerifierConfig) *LightVerifier {
	return &LightVerifier{config: cfg}
}

// Verify 验证元数据 JSON
// 1. 解析并校验元数据格式
// 2. 如果文件 < 100 MiB，验证 PoW
func (v *LightVerifier) Verify(metaJSON []byte) (*sdkmeta.Metadata, error) {
	if len(metaJSON) == 0 {
		return nil, fmt.Errorf("light verifier: empty metadata JSON")
	}

	// Step 1: 解析并校验元数据格式
	meta, err := sdkmeta.ParseAndValidate(metaJSON)
	if err != nil {
		// 尝试 Base64URL 解码
		meta, err = sdkmeta.ParseAndValidateBase64URL(string(metaJSON))
		if err != nil {
			return nil, fmt.Errorf("light verifier: metadata validation failed: %w", err)
		}
	}

	log.Debug("轻量验证器：元数据格式校验通过 root_cid=%s data_size=%d method=%s",
		meta.RootCID, meta.DataSize, meta.Method)

	// Step 2: PoW 验证（仅对小于 100 MiB 的文件）
	if !v.config.SkipPoW && meta.NeedsPoW() {
		if err := v.verifyPoW(meta); err != nil {
			return meta, fmt.Errorf("light verifier: PoW verification failed: %w", err)
		}
		log.Debug("轻量验证器：PoW 验证通过 root_cid=%s", meta.RootCID)
	} else if !meta.NeedsPoW() {
		log.Debug("轻量验证器：文件 >= 100 MiB，跳过 PoW 验证 root_cid=%s size=%d",
			meta.RootCID, meta.DataSize)
	}

	return meta, nil
}

// verifyPoW 验证工作量证明
func (v *LightVerifier) verifyPoW(meta *sdkmeta.Metadata) error {
	err := sdkpow.Verify(meta.PoW, meta.PoWAlg, meta.RootCID, meta.DataTXID, int64(meta.DataSize))
	if err != nil {
		return fmt.Errorf("PoW 验证失败: %w", err)
	}
	return nil
}

// VerifyQuick 快速验证（仅元数据格式，跳过 PoW）
func (v *LightVerifier) VerifyQuick(metaJSON []byte) (*sdkmeta.Metadata, error) {
	if len(metaJSON) == 0 {
		return nil, fmt.Errorf("light verifier: empty metadata JSON")
	}

	meta, err := sdkmeta.ParseAndValidate(metaJSON)
	if err != nil {
		meta, err = sdkmeta.ParseAndValidateBase64URL(string(metaJSON))
		if err != nil {
			return nil, fmt.Errorf("light verifier: metadata validation failed: %w", err)
		}
	}

	return meta, nil
}

// Config 返回当前配置
func (v *LightVerifier) Config() LightVerifierConfig {
	return v.config
}
