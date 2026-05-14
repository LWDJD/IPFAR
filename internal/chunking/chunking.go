// Package chunking 提供大文件 CAR 分块与合并功能
//
// 该模块实现：
//   - 大文件拆分为多个 CAR v2 分块文件
//   - 通过 reference 记录分块关系
//   - 下载所有分块 → 合并验证
//   - 分块完整性校验
//
// 规范参考: ipfar-specs/V1/数据结构规范.md §4.2 引用格式（分块场景）
package chunking

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	sdkmeta "github.com/LWDJD/ipfar-sdk/verify/metadata"

	"github.com/lwdjd/IPFAR/internal/cache"
	"github.com/lwdjd/IPFAR/internal/log"
)

// Config 分块器配置
type Config struct {
	// ChunkSize 每个分块的最大大小（字节），默认 200 MB
	ChunkSize int64
	// MaxChunks 最大分块数量，默认 100
	MaxChunks int
}

// DefaultConfig 返回默认配置
func DefaultConfig() Config {
	return Config{
		ChunkSize: 200 * 1024 * 1024, // 200 MB
		MaxChunks: 100,
	}
}

// validateConfig 校验并补全配置
func validateConfig(cfg Config) Config {
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = 200 * 1024 * 1024
	}
	if cfg.MaxChunks <= 0 {
		cfg.MaxChunks = 100
	}
	return cfg
}

// ChunkInfo 分块信息
type ChunkInfo struct {
	// TXID 分块对应的 Arweave 交易 ID（拆分时自动生成占位符）
	TXID string `json:"txid"`
	// Offset 在原始文件中的字节偏移量
	Offset int64 `json:"offset"`
	// Size 分块字节大小
	Size int64 `json:"size"`
	// CID 分块的 CID
	CID string `json:"cid"`
	// Path 分块文件本地路径
	Path string `json:"path"`
	// Index 分块序号（从 0 开始）
	Index int `json:"index"`
}

// SplitResult 拆分结果
type SplitResult struct {
	// Chunked 是否执行了拆分
	Chunked bool `json:"chunked"`
	// Chunks 分块列表
	Chunks []ChunkInfo `json:"chunks,omitempty"`
	// Metadata 包含引用关系的元数据（用于替换或补充原 metadata）
	Metadata *sdkmeta.Metadata `json:"metadata,omitempty"`
	// TotalSize 原始文件总大小
	TotalSize int64 `json:"total_size"`
}

// Chunker 大文件分块器
type Chunker struct {
	config Config
	cache  *cache.Cache
}

// NewChunker 创建分块器
func NewChunker(config Config) *Chunker {
	config = validateConfig(config)
	return &Chunker{config: config}
}

// SetCache 设置缓存实例
func (c *Chunker) SetCache(cache *cache.Cache) {
	c.cache = cache
}

// Split 拆分大文件为多个分块
//
// 如果文件大小 <= ChunkSize，不拆分，返回 Chunked=false。
// 分块文件写入 carFilePath 所在目录，命名为 {basename}.part{N}.car
func (c *Chunker) Split(carFilePath string, rootCID string) (*SplitResult, error) {
	dir := filepath.Dir(carFilePath)
	return c.SplitTo(carFilePath, rootCID, dir)
}

// SplitTo 拆分大文件到指定输出目录
func (c *Chunker) SplitTo(carFilePath string, rootCID string, outputDir string) (*SplitResult, error) {
	stat, err := os.Stat(carFilePath)
	if err != nil {
		return nil, fmt.Errorf("stat file %s: %w", carFilePath, err)
	}

	fileSize := stat.Size()
	result := &SplitResult{
		TotalSize: fileSize,
	}

	// 不需要拆分
	if fileSize <= c.config.ChunkSize {
		log.Debug("分块：文件 %s 大小 %d <= 分块大小 %d，不拆分",
			carFilePath, fileSize, c.config.ChunkSize)
		return result, nil
	}

	// 计算分块数量
	numChunks := int((fileSize + c.config.ChunkSize - 1) / c.config.ChunkSize)
	if numChunks > c.config.MaxChunks {
		return nil, fmt.Errorf("分块数量 %d 超过最大限制 %d", numChunks, c.config.MaxChunks)
	}

	result.Chunked = true

	// 打开源文件
	src, err := os.Open(carFilePath)
	if err != nil {
		return nil, fmt.Errorf("open source file: %w", err)
	}
	defer src.Close()

	// 确保输出目录存在
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("create output dir: %w", err)
	}

	baseName := filepath.Base(carFilePath)
	ext := filepath.Ext(baseName)
	nameWithoutExt := baseName[:len(baseName)-len(ext)]

	buf := make([]byte, 32*1024) // 32KB 读写缓冲

	for i := 0; i < numChunks; i++ {
		offset := int64(i) * c.config.ChunkSize
		chunkSize := c.config.ChunkSize
		if offset+chunkSize > fileSize {
			chunkSize = fileSize - offset
		}

		chunkFileName := fmt.Sprintf("%s.part%d%s", nameWithoutExt, i, ext)
		chunkPath := filepath.Join(outputDir, chunkFileName)

		// 创建分块文件
		dst, err := os.Create(chunkPath)
		if err != nil {
			return nil, fmt.Errorf("create chunk file %s: %w", chunkPath, err)
		}

		// 从源文件指定偏移量复制数据
		_, err = src.Seek(offset, io.SeekStart)
		if err != nil {
			dst.Close()
			return nil, fmt.Errorf("seek source: %w", err)
		}

		written, err := io.CopyBuffer(dst, io.LimitReader(src, chunkSize), buf)
		dst.Close()

		if err != nil {
			return nil, fmt.Errorf("write chunk %d: %w", i, err)
		}

		// 生成占位 TXID（实际由上层在提交到 Arweave 后填充）
		txidPlaceholder := fmt.Sprintf("chunk-%s-%d", rootCID[:min(16, len(rootCID))], i)

		chunk := ChunkInfo{
			TXID:   txidPlaceholder,
			Offset: offset,
			Size:   written,
			CID:    fmt.Sprintf("%s-chunk-%d", rootCID, i), // 占位 CID
			Path:   chunkPath,
			Index:  i,
		}

		result.Chunks = append(result.Chunks, chunk)

		log.Debug("分块：chunk %d/%d offset=%d size=%d path=%s",
			i+1, numChunks, offset, written, chunkPath)
	}

	log.Info("分块：完成 %s → %d 个分块 (总大小 %d)", carFilePath, numChunks, fileSize)

	return result, nil
}

// MergeChunks 合并多个分块文件为一个完整文件
func (c *Chunker) MergeChunks(chunkPaths []string, outputPath string) error {
	if len(chunkPaths) == 0 {
		return fmt.Errorf("no chunks to merge")
	}

	dst, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("create output file: %w", err)
	}
	defer dst.Close()

	buf := make([]byte, 32*1024) // 32KB buffer

	for _, chunkPath := range chunkPaths {
		src, err := os.Open(chunkPath)
		if err != nil {
			return fmt.Errorf("open chunk %s: %w", chunkPath, err)
		}

		_, err = io.CopyBuffer(dst, src, buf)
		src.Close()

		if err != nil {
			return fmt.Errorf("copy chunk %s: %w", chunkPath, err)
		}

		log.Debug("分块合并：已合并 %s", chunkPath)
	}

	// 确保写入磁盘
	if err := dst.Sync(); err != nil {
		return fmt.Errorf("sync output: %w", err)
	}

	log.Info("分块合并：完成 %d 个分块 → %s", len(chunkPaths), outputPath)
	return nil
}

// BuildChunkMetadata 构建包含分块引用关系的元数据
//
// 用于替代或补充原始元数据中的 reference 字段。
func (c *Chunker) BuildChunkMetadata(rootCID, dataTXID string, dataHeight int, chunks []ChunkInfo) *sdkmeta.Metadata {
	ref := make(sdkmeta.ReferenceMap)
	totalSize := int64(0)

	for _, chunk := range chunks {
		ref[chunk.TXID] = sdkmeta.ReferenceEntry{
			Height: dataHeight,
			CIDs:   []string{chunk.CID},
		}
		totalSize += chunk.Size
	}

	meta := &sdkmeta.Metadata{
		Version:    sdkmeta.Version1,
		Method:     sdkmeta.MethodRaw,
		RootCID:    rootCID,
		DataTXID:   dataTXID,
		DataHeight: dataHeight,
		DataSize:   int(totalSize),
		Reference:  &ref,
	}

	return meta
}

// BuildReferenceMap 构建引用映射（用于元数据 reference 字段）
func (c *Chunker) BuildReferenceMap(chunks []ChunkInfo, height int, cids []string) sdkmeta.ReferenceMap {
	ref := make(sdkmeta.ReferenceMap)
	for _, chunk := range chunks {
		ref[chunk.TXID] = sdkmeta.ReferenceEntry{
			Height: height,
			CIDs:   cids,
		}
	}
	return ref
}

// ValidateChunkIntegrity 验证单个分块的完整性
//
// 通过对比分块文件内容与源文件对应偏移量的数据来验证。
func (c *Chunker) ValidateChunkIntegrity(chunk ChunkInfo, sourceFilePath string) error {
	src, err := os.Open(sourceFilePath)
	if err != nil {
		return fmt.Errorf("open source: %w", err)
	}
	defer src.Close()

	chunkFile, err := os.Open(chunk.Path)
	if err != nil {
		return fmt.Errorf("open chunk: %w", err)
	}
	defer chunkFile.Close()

	// 从源文件指定偏移量读取
	srcData := make([]byte, chunk.Size)
	_, err = src.ReadAt(srcData, chunk.Offset)
	if err != nil {
		return fmt.Errorf("read source at offset %d: %w", chunk.Offset, err)
	}

	// 读取分块文件
	chunkData := make([]byte, chunk.Size)
	n, err := io.ReadFull(chunkFile, chunkData)
	if err != nil && err != io.ErrUnexpectedEOF {
		return fmt.Errorf("read chunk: %w", err)
	}

	if int64(n) != chunk.Size {
		return fmt.Errorf("chunk size mismatch: expected %d, got %d", chunk.Size, n)
	}

	// 逐字节对比
	for i := int64(0); i < chunk.Size; i++ {
		if srcData[i] != chunkData[i] {
			return fmt.Errorf("chunk data mismatch at offset %d", chunk.Offset+i)
		}
	}

	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
