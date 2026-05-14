package chunking

import (
	"os"
	"path/filepath"
	"testing"

	sdkmeta "github.com/LWDJD/ipfar-sdk/verify/metadata"

	"github.com/lwdjd/IPFAR/internal/cache"
)

func TestNewChunker(t *testing.T) {
	c := NewChunker(Config{
		ChunkSize: 100 * 1024 * 1024,
	})
	if c == nil {
		t.Fatal("NewChunker 不应返回 nil")
	}
	if c.config.ChunkSize != 100*1024*1024 {
		t.Errorf("ChunkSize 不匹配")
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.ChunkSize != 200*1024*1024 {
		t.Errorf("默认 ChunkSize 应为 200MB，实际 %d", cfg.ChunkSize)
	}
	if cfg.MaxChunks != 100 {
		t.Errorf("默认 MaxChunks 应为 100，实际 %d", cfg.MaxChunks)
	}
}

func TestSplitTooSmall(t *testing.T) {
	c := NewChunker(Config{
		ChunkSize: 1024 * 1024, // 1MB
	})

	// 创建一个小于分块大小的文件
	tmpDir := t.TempDir()
	smallFile := filepath.Join(tmpDir, "small.car")
	data := make([]byte, 100) // 100 bytes
	os.WriteFile(smallFile, data, 0644)

	result, err := c.Split(smallFile, "bafy123")
	if err != nil {
		t.Fatalf("Split 不应返回错误: %v", err)
	}
	if result.Chunked {
		t.Error("小于分块大小的文件不应拆分")
	}
	if len(result.Chunks) != 0 {
		t.Errorf("小文件不应有分块，实际 %d", len(result.Chunks))
	}
}

func TestSplitLargeFile(t *testing.T) {
	c := NewChunker(Config{
		ChunkSize: 500, // 500 bytes
		MaxChunks: 10,
	})

	tmpDir := t.TempDir()
	outputDir := filepath.Join(tmpDir, "chunks")

	// 创建一个大文件（~1500 bytes）
	largeFile := filepath.Join(tmpDir, "large.car")
	data := make([]byte, 1500)
	for i := range data {
		data[i] = byte(i % 256)
	}
	os.WriteFile(largeFile, data, 0644)

	rootCID := "bafy-large-file-cid"
	result, err := c.SplitTo(largeFile, rootCID, outputDir)
	if err != nil {
		t.Fatalf("SplitTo 不应返回错误: %v", err)
	}
	if !result.Chunked {
		t.Error("大于分块大小的文件应拆分")
	}
	if len(result.Chunks) == 0 {
		t.Error("应有分块")
	}

	// 验证分块数量（1500 / 500 = 3 个完整块）
	expectedChunks := 3
	if len(result.Chunks) != expectedChunks {
		t.Errorf("分块数量应为 %d，实际 %d", expectedChunks, len(result.Chunks))
	}

	// 验证总分块大小
	var totalSize int64
	for _, chunk := range result.Chunks {
		totalSize += chunk.Size
	}
	if totalSize != 1500 {
		t.Errorf("总分块大小应为 1500，实际 %d", totalSize)
	}

	// 验证分块文件存在
	for i, chunk := range result.Chunks {
		if _, err := os.Stat(chunk.Path); os.IsNotExist(err) {
			t.Errorf("分块 %d 文件不存在: %s", i, chunk.Path)
		}
	}
}

func TestMergeChunks(t *testing.T) {
	c := NewChunker(Config{
		ChunkSize: 500,
		MaxChunks: 10,
	})

	tmpDir := t.TempDir()
	outputDir := filepath.Join(tmpDir, "chunks")

	// 原始数据
	originalData := make([]byte, 1200)
	for i := range originalData {
		originalData[i] = byte((i * 7) % 256)
	}

	largeFile := filepath.Join(tmpDir, "large.car")
	os.WriteFile(largeFile, originalData, 0644)

	// 拆分
	rootCID := "bafy-merge-test"
	result, err := c.SplitTo(largeFile, rootCID, outputDir)
	if err != nil {
		t.Fatalf("SplitTo 失败: %v", err)
	}

	// 构建分块列表用于合并
	var chunkPaths []string
	for _, chunk := range result.Chunks {
		chunkPaths = append(chunkPaths, chunk.Path)
	}

	// 合并
	mergedPath := filepath.Join(tmpDir, "merged.car")
	err = c.MergeChunks(chunkPaths, mergedPath)
	if err != nil {
		t.Fatalf("MergeChunks 失败: %v", err)
	}

	// 验证合并后的文件
	mergedData, err := os.ReadFile(mergedPath)
	if err != nil {
		t.Fatalf("读取合并文件失败: %v", err)
	}
	if len(mergedData) != len(originalData) {
		t.Errorf("合并后大小应为 %d，实际 %d", len(originalData), len(mergedData))
	}
	for i := range originalData {
		if mergedData[i] != originalData[i] {
			t.Errorf("合并数据在偏移 %d 处不匹配: got %d, want %d", i, mergedData[i], originalData[i])
			break
		}
	}
}

func TestBuildChunkMetadata(t *testing.T) {
	c := NewChunker(Config{
		ChunkSize: 1024 * 1024,
	})

	rootCID := "bafy-root-cid"
	dataTXID := "tx-main-data"
	dataHeight := 1000

	chunks := []ChunkInfo{
		{TXID: "tx-chunk-1", Offset: 0, Size: 500, CID: "cid-1"},
		{TXID: "tx-chunk-2", Offset: 500, Size: 500, CID: "cid-2"},
	}

	meta := c.BuildChunkMetadata(rootCID, dataTXID, dataHeight, chunks)
	if meta == nil {
		t.Fatal("BuildChunkMetadata 不应返回 nil")
	}
	if meta.RootCID != rootCID {
		t.Errorf("RootCID 不匹配")
	}
	if meta.DataTXID != dataTXID {
		t.Errorf("DataTXID 不匹配")
	}

	// 验证 reference 包含分块 TXID
	if !meta.HasReference() {
		t.Fatal("元数据应包含 reference")
	}

	ref := *meta.Reference
	for _, chunk := range chunks {
		entry, ok := ref[chunk.TXID]
		if !ok {
			t.Errorf("reference 应包含 %s", chunk.TXID)
			continue
		}
		if entry.Height != dataHeight {
			t.Errorf("chunk %s height 应为 %d", chunk.TXID, dataHeight)
		}
	}
}

func TestValidateChunkIntegrity(t *testing.T) {
	c := NewChunker(Config{
		ChunkSize: 500,
	})

	tmpDir := t.TempDir()

	// 创建原始数据
	data := make([]byte, 800)
	for i := range data {
		data[i] = byte(i % 256)
	}

	srcFile := filepath.Join(tmpDir, "src.car")
	os.WriteFile(srcFile, data, 0644)

	// 拆分
	result, err := c.SplitTo(srcFile, "bafy-integrity", filepath.Join(tmpDir, "chunks"))
	if err != nil {
		t.Fatalf("SplitTo 失败: %v", err)
	}

	// 验证每个分块的完整性
	for _, chunk := range result.Chunks {
		err := c.ValidateChunkIntegrity(chunk, srcFile)
		if err != nil {
			t.Errorf("分块完整性验证失败 %s: %v", chunk.TXID, err)
		}
	}

	// 验证引用元数据（手动构建）
	refMeta := c.BuildChunkMetadata("bafy-integrity", "tx-source", 1000, result.Chunks)
	if refMeta == nil {
		t.Fatal("BuildChunkMetadata 不应为 nil")
	}
	if refMeta.RootCID != "bafy-integrity" {
		t.Errorf("metadata RootCID 不匹配")
	}
	if !refMeta.HasReference() {
		t.Error("metadata 应有 reference")
	}
}

func TestBuildReferenceMap(t *testing.T) {
	c := NewChunker(DefaultConfig())

	chunks := []ChunkInfo{
		{TXID: "tx-a", Offset: 0, Size: 100, CID: "cid-a"},
		{TXID: "tx-b", Offset: 100, Size: 200, CID: "cid-b"},
	}

	height := 5000
	cids := []string{"cid-a", "cid-b"}

	refMap := c.BuildReferenceMap(chunks, height, cids)
	if len(refMap) != 2 {
		t.Errorf("ReferenceMap 应有 2 个条目，实际 %d", len(refMap))
	}

	if refMap["tx-a"].Height != height {
		t.Errorf("tx-a height 应为 %d", height)
	}
	if len(refMap["tx-a"].CIDs) != 2 {
		t.Errorf("tx-a CIDs 应有 2 个")
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name   string
		config Config
	}{
		{"valid", Config{ChunkSize: 100 * 1024 * 1024, MaxChunks: 50}},
		{"zero chunk size", Config{ChunkSize: 0, MaxChunks: 50}},
		{"zero max chunks", Config{ChunkSize: 100 * 1024 * 1024, MaxChunks: 0}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewChunker(tt.config)
			if c.config.ChunkSize <= 0 {
				t.Error("ChunkSize 应 > 0")
			}
			if c.config.MaxChunks <= 0 {
				t.Error("MaxChunks 应 > 0")
			}
		})
	}
}

var _ = cache.DefaultCacheConfig
var _ = sdkmeta.Version1
