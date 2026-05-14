// Package store 提供基于 Badger v4 的嵌入式 KV 存储。
//
// 用于替换/补充当前的 JSONL 存储，提供：
//   - 元数据索引（Metadata Index）
//   - 已验证的 CID 列表（Verified CID List）
//   - 文件缓存路径映射（Cache Path Mapping）
//
// 内存使用：严格控制在 1.6 GiB 限制内。
// Badger 配置了保守的内存表大小、块缓存和索引缓存。
//
// 线程安全：所有操作都是并发安全的（Badger 原生支持）。
package store

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/dgraph-io/badger/v4"

	"github.com/lwdjd/IPFAR/internal/log"
)

// StoreConfig KV 存储配置
type StoreConfig struct {
	// Dir 数据目录
	Dir string
	// MemTableSize 单个 memtable 最大大小（字节），默认 16MB
	MemTableSize int64
	// NumMemtables 最大 memtable 数量，默认 2
	NumMemtables int
	// BlockCacheSize 块缓存大小（字节），默认 32MB
	BlockCacheSize int64
	// IndexCacheSize 索引缓存大小（字节），默认 8MB
	IndexCacheSize int64
	// NumCompactors 压缩器数量，默认 2
	NumCompactors int
	// ValueLogFileSize 值日志文件大小（字节），默认 64MB
	ValueLogFileSize int64
	// ValueThreshold 值阈值：小于此值存 LSM tree，否则存 value log。默认 1KB
	ValueThreshold int64
	// SyncWrites 是否同步写入（默认 false，性能更好）
	SyncWrites bool
	// ReadOnly 只读模式
	ReadOnly bool
}

// DefaultStoreConfig 返回适合 1.6 GiB 内存限制的默认配置
//
// 内存估算：
//   - 2 memtables × 16MB = 32MB
//   - Block cache: 32MB
//   - Index cache: 8MB
//   - 压缩缓冲: ~16MB
//   - 总计: ~88MB（远低于 1.6 GiB 限制）
func DefaultStoreConfig() StoreConfig {
	return StoreConfig{
		Dir:              "data/badger",
		MemTableSize:     16 << 20, // 16 MB
		NumMemtables:     2,
		BlockCacheSize:   32 << 20, // 32 MB
		IndexCacheSize:   8 << 20,  // 8 MB
		NumCompactors:    2,
		ValueLogFileSize: 64 << 20,  // 64 MB
		ValueThreshold:   1 << 10,   // 1 KB
		SyncWrites:       false,
		ReadOnly:         false,
	}
}

// Store Badger KV 存储
type Store struct {
	db   *badger.DB
	dir  string
	conf StoreConfig
}

// New 创建或打开 KV 存储
func New(config StoreConfig) (*Store, error) {
	if config.Dir == "" {
		config.Dir = DefaultStoreConfig().Dir
	}

	// 确保目录存在
	if err := os.MkdirAll(config.Dir, 0755); err != nil {
		return nil, fmt.Errorf("创建存储目录失败 %s: %w", config.Dir, err)
	}

	opts := badger.DefaultOptions(config.Dir)

	// 应用自定义配置
	if config.MemTableSize > 0 {
		opts.MemTableSize = config.MemTableSize
	}
	if config.NumMemtables > 0 {
		opts.NumMemtables = config.NumMemtables
	}
	if config.BlockCacheSize > 0 {
		opts.BlockCacheSize = config.BlockCacheSize
	}
	if config.IndexCacheSize > 0 {
		opts.IndexCacheSize = config.IndexCacheSize
	}
	if config.NumCompactors > 0 {
		opts.NumCompactors = config.NumCompactors
	}
	if config.ValueLogFileSize > 0 {
		opts.ValueLogFileSize = config.ValueLogFileSize
	}
	if config.ValueThreshold > 0 {
		opts.ValueThreshold = config.ValueThreshold
	}
	opts.SyncWrites = config.SyncWrites
	opts.ReadOnly = config.ReadOnly

	// 日志级别
	opts.Logger = nil // 使用默认 logger（静默）

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("打开 Badger 数据库失败: %w", err)
	}

	s := &Store{
		db:   db,
		dir:  config.Dir,
		conf: config,
	}

	log.Info("存储：Badger KV 已打开 dir=%s memtable=%dMB blockcache=%dMB indexcache=%dMB",
		config.Dir, config.MemTableSize>>20, config.BlockCacheSize>>20, config.IndexCacheSize>>20)

	return s, nil
}

// Set 设置键值对
func (s *Store) Set(key, value []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

// SetString 设置字符串键值对（便捷方法）
func (s *Store) SetString(key, value string) error {
	return s.Set([]byte(key), []byte(value))
}

// Get 获取键对应的值
// 返回 (value, found, error)
func (s *Store) Get(key []byte) ([]byte, bool, error) {
	var val []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return err
		}
		return item.Value(func(v []byte) error {
			val = append([]byte{}, v...) // 复制数据
			return nil
		})
	})
	if err != nil {
		return nil, false, err
	}
	if val == nil {
		return nil, false, nil
	}
	return val, true, nil
}

// GetString 获取字符串值（便捷方法）
func (s *Store) GetString(key string) (string, bool, error) {
	val, found, err := s.Get([]byte(key))
	if err != nil || !found {
		return "", found, err
	}
	return string(val), true, nil
}

// Has 检查键是否存在
func (s *Store) Has(key []byte) (bool, error) {
	found := false
	err := s.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return err
		}
		found = true
		return nil
	})
	return found, err
}

// HasString 检查字符串键是否存在（便捷方法）
func (s *Store) HasString(key string) (bool, error) {
	return s.Has([]byte(key))
}

// Delete 删除键值对
func (s *Store) Delete(key []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

// DeleteString 删除字符串键（便捷方法）
func (s *Store) DeleteString(key string) error {
	return s.Delete([]byte(key))
}

// BatchSet 批量设置键值对
// 比多次调用 Set 更高效
func (s *Store) BatchSet(pairs map[string]string) error {
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()

	for k, v := range pairs {
		if err := wb.Set([]byte(k), []byte(v)); err != nil {
			return fmt.Errorf("批量写入键 %s 失败: %w", k, err)
		}
	}

	return wb.Flush()
}

// BatchSetBytes 批量设置字节键值对
func (s *Store) BatchSetBytes(pairs map[string][]byte) error {
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()

	for k, v := range pairs {
		if err := wb.Set([]byte(k), v); err != nil {
			return fmt.Errorf("批量写入键 %s 失败: %w", k, err)
		}
	}

	return wb.Flush()
}

// PrefixScan 扫描指定前缀的所有键值对
// 回调函数 fn 返回 false 时停止扫描
func (s *Store) PrefixScan(prefix []byte, fn func(key, value []byte) bool) error {
	return s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)
			err := item.Value(func(v []byte) error {
				if !fn(key, v) {
					// 返回一个特殊错误来停止迭代
					return errStopIteration
				}
				return nil
			})
			if err != nil {
				if err == errStopIteration {
					return nil
				}
				return err
			}
		}
		return nil
	})
}

// errStopIteration 内部错误，用于提前停止迭代
var errStopIteration = fmt.Errorf("stop iteration")

// PrefixScanKeys 扫描指定前缀的所有键
func (s *Store) PrefixScanKeys(prefix []byte) ([][]byte, error) {
	var keys [][]byte
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().KeyCopy(nil)
			keys = append(keys, key)
		}
		return nil
	})
	return keys, err
}

// Count 统计指定前缀的键数量
func (s *Store) Count(prefix []byte) (int, error) {
	count := 0
	err := s.PrefixScan(prefix, func(key, value []byte) bool {
		count++
		return true
	})
	return count, err
}

// DropPrefix 删除指定前缀的所有键值对
func (s *Store) DropPrefix(prefix []byte) error {
	keys, err := s.PrefixScanKeys(prefix)
	if err != nil {
		return fmt.Errorf("扫描前缀失败: %w", err)
	}

	if len(keys) == 0 {
		return nil
	}

	wb := s.db.NewWriteBatch()
	defer wb.Cancel()

	for _, key := range keys {
		if err := wb.Delete(key); err != nil {
			return fmt.Errorf("批量删除键失败: %w", err)
		}
	}

	return wb.Flush()
}

// Close 关闭存储
func (s *Store) Close() error {
	log.Info("存储：正在关闭 Badger...")
	err := s.db.Close()
	if err != nil {
		return fmt.Errorf("关闭 Badger 失败: %w", err)
	}
	log.Info("存储：Badger 已关闭")
	return nil
}

// RunGC 运行垃圾回收（可在后台 goroutine 中定期调用）
func (s *Store) RunGC() error {
	return s.db.RunValueLogGC(0.5) // 当 50% 空间可回收时运行
}

// Size 返回存储目录的磁盘使用量（近似）
func (s *Store) Size() (int64, error) {
	var size int64
	err := filepath.Walk(s.dir, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

// Backup 备份数据库到指定文件
// 写入 protobuf 编码的条目列表到 writer
func (s *Store) Backup(w io.Writer, since uint64) (uint64, error) {
	return s.db.Backup(w, since)
}

// ============================================================
// 预定义键前缀（用于组织数据）
// ============================================================

const (
	// PrefixMeta 元数据索引: "meta:{txid}" → JSON metadata
	PrefixMeta = "meta:"
	// PrefixVerifiedCID 已验证的 CID: "verified:{cid}" → timestamp
	PrefixVerifiedCID = "verified:"
	// PrefixCachePath 缓存路径映射: "cache:{cid}" → filepath
	PrefixCachePath = "cache:"
	// PrefixConfig 配置项: "config:{key}" → value
	PrefixConfig = "config:"
)

// MetaKey 构建元数据键
func MetaKey(txID string) []byte {
	return []byte(PrefixMeta + txID)
}

// VerifiedCIDKey 构建已验证 CID 键
func VerifiedCIDKey(cid string) []byte {
	return []byte(PrefixVerifiedCID + cid)
}

// CachePathKey 构建缓存路径键
func CachePathKey(cid string) []byte {
	return []byte(PrefixCachePath + cid)
}

// ConfigKey 构建配置键
func ConfigKey(key string) []byte {
	return []byte(PrefixConfig + key)
}
