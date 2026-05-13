package download

import (
	"fmt"
	"os"
	"path/filepath"

	sdkmeta "github.com/LWDJD/ipfar-sdk/verify/metadata"

	"github.com/lwdjd/IPFAR/internal/log"
)

// FetcherConfig 下载器配置
type FetcherConfig struct {
	// Gateway 网关客户端（若为 nil 则使用默认网关）
	Gateway *Gateway
	// CacheDir CAR 文件缓存目录
	CacheDir string
	// MaxFileSize 最大下载文件大小（字节），0 表示不限制
	MaxFileSize int64
}

// DefaultFetcherConfig 返回默认下载器配置
func DefaultFetcherConfig() FetcherConfig {
	return FetcherConfig{
		Gateway:     nil, // 延迟初始化
		CacheDir:    "cache/car",
		MaxFileSize: 200 * 1024 * 1024, // 200 MB
	}
}

// Fetcher 元数据→CAR 下载链
// 实现完整的下载链路：获取元数据交易 → 解析 data_txid → 下载 CAR 文件
type Fetcher struct {
	config  FetcherConfig
	gateway *Gateway
}

// NewFetcher 创建新的下载器
func NewFetcher(config FetcherConfig) *Fetcher {
	if config.Gateway != nil {
		return &Fetcher{
			config:  config,
			gateway: config.Gateway,
		}
	}

	return &Fetcher{
		config:  config,
		gateway: NewGateway(DefaultGatewayConfig()),
	}
}

// FetchMetadataByTXID 根据交易 ID 获取并解析元数据
// 完整链路：txID → 网关下载 → Base64URL 解码 → JSON 解析 → 验证 → Metadata
func (f *Fetcher) FetchMetadataByTXID(txID string) (*sdkmeta.Metadata, error) {
	log.Info("下载：获取元数据交易 %s", txID)

	data, err := f.gateway.FetchTransaction(txID)
	if err != nil {
		return nil, fmt.Errorf("下载元数据交易 %s 失败: %w", txID, err)
	}

	// Arweave 返回的交易数据可能是原始 JSON 或 Base64URL 编码
	// 先尝试作为 JSON 解析
	meta, err := sdkmeta.ParseAndValidate(data)
	if err != nil {
		// 尝试作为 Base64URL 解码
		meta, err = sdkmeta.ParseAndValidateBase64URL(string(data))
		if err != nil {
			return nil, fmt.Errorf("解析元数据失败（既不是 JSON 也不是 Base64URL）: %w", err)
		}
	}

	log.Info("下载：元数据解析成功 root_cid=%s data_txid=%s data_size=%d",
		meta.RootCID, meta.DataTXID, meta.DataSize)

	return meta, nil
}

// FetchMetadataFromJSON 从 JSON 字节解析元数据
func (f *Fetcher) FetchMetadataFromJSON(jsonData []byte) (*sdkmeta.Metadata, error) {
	meta, err := sdkmeta.ParseAndValidate(jsonData)
	if err != nil {
		return nil, fmt.Errorf("解析元数据 JSON 失败: %w", err)
	}
	return meta, nil
}

// FetchMetadataFromBase64 从 Base64URL 字符串解析元数据
func (f *Fetcher) FetchMetadataFromBase64(encoded string) (*sdkmeta.Metadata, error) {
	meta, err := sdkmeta.ParseAndValidateBase64URL(encoded)
	if err != nil {
		return nil, fmt.Errorf("解析元数据 Base64URL 失败: %w", err)
	}
	return meta, nil
}

// DownloadCAR 根据元数据下载 CAR 文件
// 使用 meta.DataTXID 从 Arweave 网关下载 CAR 文件
func (f *Fetcher) DownloadCAR(meta *sdkmeta.Metadata) (string, error) {
	if meta == nil {
		return "", fmt.Errorf("metadata is nil")
	}

	dataTXID := meta.DataTXID
	if dataTXID == "" {
		return "", fmt.Errorf("data_txid is empty")
	}

	// 检查文件大小
	if f.config.MaxFileSize > 0 && int64(meta.DataSize) > f.config.MaxFileSize {
		return "", fmt.Errorf("文件大小 %d 超过限制 %d 字节", meta.DataSize, f.config.MaxFileSize)
	}

	// 检查缓存
	cachePath := f.cachePath(meta)
	if _, err := os.Stat(cachePath); err == nil {
		log.Info("下载：CAR 文件已缓存 %s", cachePath)
		return cachePath, nil
	}

	log.Info("下载：开始下载 CAR 文件 data_txid=%s size=%d bytes", dataTXID, meta.DataSize)

	// 从网关下载
	data, err := f.gateway.FetchTransactionData(dataTXID)
	if err != nil {
		return "", fmt.Errorf("下载 CAR 文件 %s 失败: %w", dataTXID, err)
	}

	// 确保缓存目录存在
	if err := os.MkdirAll(f.config.CacheDir, 0755); err != nil {
		return "", fmt.Errorf("创建缓存目录失败: %w", err)
	}

	// 写入缓存
	if err := os.WriteFile(cachePath, data, 0644); err != nil {
		return "", fmt.Errorf("写入 CAR 缓存文件失败: %w", err)
	}

	log.Info("下载：CAR 文件已保存 %s (%d bytes)", cachePath, len(data))

	return cachePath, nil
}

// DownloadCARWithProgress 带进度的 CAR 文件下载
func (f *Fetcher) DownloadCARWithProgress(meta *sdkmeta.Metadata, onProgress func(downloaded, total int64)) (string, error) {
	// 简化实现：因网关通常不支持进度回调，这里直接调用 DownloadCAR
	// 后续可通过分片下载实现真正进度
	if onProgress != nil {
		onProgress(0, int64(meta.DataSize))
	}

	cachePath, err := f.DownloadCAR(meta)

	if onProgress != nil {
		if err == nil {
			info, statErr := os.Stat(cachePath)
			if statErr == nil {
				onProgress(info.Size(), int64(meta.DataSize))
			}
		}
	}

	return cachePath, err
}

// FetchAndDownload 完整链路：从 txID 获取元数据并下载 CAR 文件
// 返回 (metadata, carFilePath, error)
func (f *Fetcher) FetchAndDownload(txID string) (*sdkmeta.Metadata, string, error) {
	meta, err := f.FetchMetadataByTXID(txID)
	if err != nil {
		return nil, "", fmt.Errorf("获取元数据失败: %w", err)
	}

	cachePath, err := f.DownloadCAR(meta)
	if err != nil {
		return meta, "", fmt.Errorf("下载 CAR 文件失败: %w", err)
	}

	return meta, cachePath, nil
}

// DownloadReferenceData 下载引用链中的所有引用数据
// 返回 (引用TXID → 本地文件路径) 映射
func (f *Fetcher) DownloadReferenceData(meta *sdkmeta.Metadata) (map[string]string, error) {
	if !meta.HasReference() {
		return nil, nil
	}

	result := make(map[string]string)
	ref := *meta.Reference

	for txID := range ref {
		data, err := f.gateway.FetchTransactionData(txID)
		if err != nil {
			log.Warn("下载：引用数据下载失败 txID=%s: %v", txID, err)
			continue
		}

		refPath := filepath.Join(f.config.CacheDir, "ref", txID)
		if err := os.MkdirAll(filepath.Dir(refPath), 0755); err != nil {
			return result, fmt.Errorf("创建引用缓存目录失败: %w", err)
		}
		if err := os.WriteFile(refPath, data, 0644); err != nil {
			return result, fmt.Errorf("写入引用数据失败: %w", err)
		}
		result[txID] = refPath
	}

	return result, nil
}

// GetGateway 获取网关客户端（用于直接访问）
func (f *Fetcher) GetGateway() *Gateway {
	return f.gateway
}

// cachePath 生成 CAR 文件缓存路径
// 使用 root_cid 的哈希避免文件名太长
func (f *Fetcher) cachePath(meta *sdkmeta.Metadata) string {
	// 使用 root_cid 和 data_txid 的前 12 字符作为文件名
	shortCID := meta.RootCID
	if len(shortCID) > 32 {
		shortCID = shortCID[:32]
	}
	shortTXID := meta.DataTXID
	if len(shortTXID) > 12 {
		shortTXID = shortTXID[:12]
	}
	filename := fmt.Sprintf("%s_%s.car", shortCID, shortTXID)
	// 清理文件名中的非法字符
	filename = sanitizeFilename(filename)
	return filepath.Join(f.config.CacheDir, filename)
}

// sanitizeFilename 清理文件名中的非法字符
func sanitizeFilename(name string) string {
	// 替换路径分隔符
	result := make([]byte, 0, len(name))
	for _, c := range []byte(name) {
		switch c {
		case '/', '\\', ':', '*', '?', '"', '<', '>', '|':
			result = append(result, '_')
		default:
			result = append(result, c)
		}
	}
	return string(result)
}

// ClearCache 清除 CAR 文件缓存
func (f *Fetcher) ClearCache() error {
	if f.config.CacheDir != "" {
		return os.RemoveAll(f.config.CacheDir)
	}
	return nil
}
