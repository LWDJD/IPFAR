package bridge

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/LWDJD/ipfar-sdk/verify/metadata"
	"github.com/LWDJD/ipfar-sdk/verify/pipeline"
)

// ============================================================
// NewBridge 测试
// ============================================================

func TestNewBridge(t *testing.T) {
	b := NewBridge(true, false, true, false)
	if b == nil {
		t.Fatal("NewBridge returned nil")
	}

	config := b.GetConfig()
	if !config.VerifyPoW {
		t.Error("VerifyPoW should be true")
	}
	if config.VerifyIndex {
		t.Error("VerifyIndex should be false")
	}
	if !config.VerifyReferenceChain {
		t.Error("VerifyReferenceChain should be true")
	}
	if config.VerifyIntegrity {
		t.Error("VerifyIntegrity should be false")
	}
}

func TestNewBridgeFromPreset(t *testing.T) {
	tests := []struct {
		preset   string
		wantPoW  bool
		wantIdx  bool
		wantRef  bool
		wantInt  bool
		wantErr  bool
	}{
		{"strict", true, true, true, true, false},
		{"balanced", true, true, false, true, false},
		{"light", true, true, true, false, false},
		{"trusted", false, true, false, false, false},
		{"invalid", false, false, false, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.preset, func(t *testing.T) {
			b, err := NewBridgeFromPreset(tt.preset)
			if tt.wantErr {
				if err == nil {
					t.Fatal("Expected error for invalid preset")
				}
				return
			}
			if err != nil {
				t.Fatalf("NewBridgeFromPreset failed: %v", err)
			}

			config := b.GetConfig()
			if config.VerifyPoW != tt.wantPoW {
				t.Errorf("VerifyPoW = %v, want %v", config.VerifyPoW, tt.wantPoW)
			}
			if config.VerifyIndex != tt.wantIdx {
				t.Errorf("VerifyIndex = %v, want %v", config.VerifyIndex, tt.wantIdx)
			}
		})
	}
}

// ============================================================
// VerifyMetadata 测试
// ============================================================

func TestVerifyMetadata_Valid(t *testing.T) {
	meta := validMetaJSON()
	b := NewBridge(true, true, true, true)

	result, err := b.VerifyMetadata(meta)
	if err != nil {
		t.Fatalf("VerifyMetadata failed: %v", err)
	}
	if result.RootCID != "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi" {
		t.Error("RootCID mismatch")
	}
}

func TestVerifyMetadata_Invalid(t *testing.T) {
	b := NewBridge(true, true, true, true)

	_, err := b.VerifyMetadata([]byte(`{"version": 99}`))
	if err == nil {
		t.Fatal("Expected error for invalid metadata")
	}
}

func TestVerifyMetadata_Empty(t *testing.T) {
	b := NewBridge(true, true, true, true)

	_, err := b.VerifyMetadata([]byte{})
	if err == nil {
		t.Fatal("Expected error for empty metadata")
	}
}

func TestVerifyMetadataFromBase64_Valid(t *testing.T) {
	jsonStr := `{"version":1,"method":"raw","root_cid":"bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi","data_txid":"tx12345678901234567890123456789012345678901","data_height":100,"data_size":200000000}`
	encoded := base64.RawURLEncoding.EncodeToString([]byte(jsonStr))

	b := NewBridge(true, true, true, true)
	result, err := b.VerifyMetadataFromBase64(encoded)
	if err != nil {
		t.Fatalf("VerifyMetadataFromBase64 failed: %v", err)
	}
	if result.Version != 1 {
		t.Error("Version mismatch")
	}
}

func TestVerifyMetadataFromBase64_Invalid(t *testing.T) {
	b := NewBridge(true, true, true, true)

	_, err := b.VerifyMetadataFromBase64("!!!invalid!!!")
	if err == nil {
		t.Fatal("Expected error for invalid Base64")
	}
}

// ============================================================
// VerifyPoW 测试
// ============================================================

func TestVerifyPoW_LargeFile(t *testing.T) {
	b := NewBridge(true, true, true, true)

	meta := &metadata.Metadata{
		Version:    1,
		Method:     "raw",
		RootCID:    "bafytest",
		DataTXID:   "tx12345678901234567890123456789012345678901",
		DataHeight: 100,
		DataSize:   200 * 1024 * 1024, // 200 MiB
	}

	err := b.VerifyPoW(meta)
	if err != nil {
		t.Errorf("Large file should skip PoW: %v", err)
	}
}

func TestVerifyPoW_NilMetadata(t *testing.T) {
	b := NewBridge(true, true, true, true)

	err := b.VerifyPoW(nil)
	if err == nil {
		t.Fatal("Expected error for nil metadata")
	}
}

func TestVerifyPoW_SmallFileNoPoW(t *testing.T) {
	b := NewBridge(true, true, true, true)

	meta := &metadata.Metadata{
		Version:    1,
		Method:     "raw",
		RootCID:    "bafytest",
		DataTXID:   "tx12345678901234567890123456789012345678901",
		DataHeight: 100,
		DataSize:   1024, // 1 KiB, needs PoW
		// Missing PoW and PoWAlg
	}

	err := b.VerifyPoW(meta)
	if err == nil {
		t.Fatal("Expected error for small file without PoW")
	}
}

// ============================================================
// RunPipeline 测试
// ============================================================

func TestRunPipeline_ValidMetadata(t *testing.T) {
	meta := &metadata.Metadata{
		Version:    1,
		Method:     "raw",
		RootCID:    "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi",
		DataTXID:   "arweave_tx_id_1234567890123456789012345678901234567890",
		DataHeight: 1913000,
		DataSize:   200 * 1024 * 1024, // 200 MiB
	}

	b := NewBridge(true, true, true, true)
	result := b.RunPipeline(meta, false)

	if !result.Passed {
		t.Error("Valid metadata should pass pipeline")
	}
}

func TestRunPipeline_InvalidMetadata(t *testing.T) {
	meta := &metadata.Metadata{
		Version: 99, // Invalid
	}

	b := NewBridge(true, true, true, true)
	result := b.RunPipeline(meta, false)

	if result.Passed {
		t.Fatal("Invalid metadata should fail pipeline")
	}
}

// ============================================================
// PrintPipelineResult 测试
// ============================================================

func TestPrintPipelineResult(t *testing.T) {
	result := &pipeline.PipelineResult{
		Passed: true,
		Results: []pipeline.VerifyResult{
			{Step: "meta_validate", Passed: true},
			{Step: "pow", Passed: true, Skipped: true, Message: "file size >= 100 MiB, PoW not required"},
		},
	}

	output := PrintPipelineResult(result)
	if !strings.Contains(output, "通过") {
		t.Error("Output should contain '通过'")
	}
	if !strings.Contains(output, "meta_validate") {
		t.Error("Output should contain step names")
	}
}

func TestPrintPipelineResult_Failed(t *testing.T) {
	result := &pipeline.PipelineResult{
		Passed: false,
		Results: []pipeline.VerifyResult{
			{Step: "meta_validate", Passed: false, Error: "invalid version"},
		},
	}

	output := PrintPipelineResult(result)
	if !strings.Contains(output, "失败") {
		t.Error("Output should contain '失败'")
	}
}

func TestPrintPipelineResult_Nil(t *testing.T) {
	output := PrintPipelineResult(nil)
	if output != "无验证结果" {
		t.Errorf("Nil result should return '无验证结果', got: %s", output)
	}
}

// ============================================================
// FormatMetadataJSON 测试
// ============================================================

func TestFormatMetadataJSON(t *testing.T) {
	meta := &metadata.Metadata{
		Version:    1,
		Method:     "raw",
		RootCID:    "bafytest",
		DataTXID:   "tx12345678901234567890123456789012345678901",
		DataHeight: 100,
		DataSize:   200000000,
	}

	output, err := FormatMetadataJSON(meta)
	if err != nil {
		t.Fatalf("FormatMetadataJSON failed: %v", err)
	}

	if !strings.Contains(output, "bafytest") {
		t.Error("JSON output should contain root CID")
	}

	// 验证是合法的 JSON
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(output), &m); err != nil {
		t.Errorf("Output is not valid JSON: %v", err)
	}
}

func TestFormatMetadataJSON_Nil(t *testing.T) {
	_, err := FormatMetadataJSON(nil)
	if err == nil {
		t.Fatal("Expected error for nil metadata")
	}
}

// ============================================================
// GetConfig 测试
// ============================================================

func TestGetConfig(t *testing.T) {
	b := NewBridge(true, false, true, false)
	config := b.GetConfig()

	if config != (pipeline.VerifyConfig{
		VerifyPoW:            true,
		VerifyIndex:          false,
		VerifyReferenceChain: true,
		VerifyIntegrity:      false,
	}) {
		t.Error("GetConfig returned unexpected config")
	}
}

// ============================================================
// GetPoWInfo 测试
// ============================================================

func TestGetPoWInfo(t *testing.T) {
	info := GetPoWInfo()
	if info.Algorithm != "argon2id-light-v1" {
		t.Error("Algorithm mismatch")
	}
	if info.MinLeadingZeroBytes != 2 {
		t.Error("MinLeadingZeroBytes mismatch")
	}
	if info.Threshold != "100 MiB" {
		t.Error("Threshold mismatch")
	}
}

// ============================================================
// 集成场景测试
// ============================================================

func TestIntegration_FullFlow(t *testing.T) {
	// 模拟完整的桥接流程：
	// 1. 收到元数据 JSON
	// 2. 解析验证元数据
	// 3. 运行验证管道

	jsonData := validMetaJSON()
	b := NewBridge(true, true, true, true)

	// Step 1-2: 验证元数据
	meta, err := b.VerifyMetadata(jsonData)
	if err != nil {
		t.Fatalf("Step 1-2 failed: %v", err)
	}

	// Step 3: 运行管道（CAR 未下载）
	result := b.RunPipeline(meta, false)
	if !result.Passed {
		t.Errorf("Pipeline should pass for valid metadata: %+v", result.Results)
	}

	// 验证元数据字段
	if meta.Version != 1 {
		t.Error("Version mismatch")
	}
	if meta.Method != "raw" {
		t.Error("Method mismatch")
	}
}

func TestIntegration_MetadataWithReference(t *testing.T) {
	jsonStr := `{
		"version": 1,
		"method": "raw",
		"root_cid": "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi",
		"data_txid": "tx12345678901234567890123456789012345678901",
		"data_height": 100,
		"data_size": 5000000,
		"reference": {
			"chunk123456789012345678901234567890123456789012": {
				"height": 101,
				"cids": ["bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"]
			}
		},
		"pow": "42",
		"pow_alg": "argon2id-light-v1"
	}`

	b := NewBridge(true, true, true, true)
	meta, err := b.VerifyMetadata([]byte(jsonStr))
	if err != nil {
		t.Fatalf("VerifyMetadata with reference failed: %v", err)
	}

	if !meta.HasReference() {
		t.Error("Metadata should have reference")
	}
}

// ============================================================
// 辅助函数
// ============================================================

func validMetaJSON() []byte {
	return []byte(`{
		"version": 1,
		"method": "raw",
		"root_cid": "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi",
		"data_txid": "arweave_tx_id_1234567890123456789012345678901234567890",
		"data_height": 1913000,
		"data_size": 200000000
	}`)
}
