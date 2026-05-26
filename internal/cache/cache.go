// Package cache 提供本地文件缓存功能，支持 LRU 驱逐策略。
//
// 该模块为 IPFAR 桥节点提供以下能力：
//   - CAR 文件缓存（避免重复从 Arweave 网关下载）
//   - 元数据缓存（避免重复解析和验证）
//   - Bitswap 块缓存（从 Bitswap 请求的块自动写入缓存）
//   - 文件系统 + LRU 驱逐策略
//   - 可配置大小上限
//   - 线程安全
//
// 规范参考: ipfar-specs/V1/项目规划.md §四.6
package cache

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lwdjd/IPFAR/internal/log"
)

// indexFileName 持久化索引文件名
const indexFileName = "cache_index.json"

// CacheConfig 缓存配置
type CacheConfig struct {
	// Dir 缓存文件目录
	Dir string
	// MaxSize 最大缓存大小（字节），0 表示无限制
	MaxSize int64
}

// DefaultCacheConfig 返回默认缓存配置（500 MB）
func DefaultCacheConfig() CacheConfig {
	return CacheConfig{
		Dir:     "cache/ipfar",
		MaxSize: 500 * 1024 * 1024, // 500 MB
	}
}

// entryMeta 缓存条目元数据（写入索引文件）
type entryMeta struct {
	Key        string    `json:"key"`
	FilePath    string    `json:"file_path"`
	Size       int64     `json:"size"`
	LastAccess time.Time `json:"last_access"`
}

// lruNode LRU 双向链表节点
type lruNode struct {
	key  string
	prev *lruNode
	next *lruNode
}

// Cache 本地文件缓存
type Cache struct {
	dir     string
	maxSize int64

	mu      sync.RWMutex
	entries map[string]*entryMeta // key → entry

	// LRU 链表（双向链表）
	lruHead *lruNode
	lruTail *lruNode

	// 统计
	currentSize  int64
	totalEntries int64
	hits         int64
	misses       int64
	evictions    int64

	// 持久化
	dirty bool
}

// New 创建新的缓存实例
// 如果配置的目录已存在旧索引，会自动加载。
func New(config CacheConfig) (*Cache, error) {
	dir := config.Dir
	if dir == "" {
		dir = DefaultCacheConfig().Dir
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("创建缓存目录失败 %s: %w", dir, err)
	}

	c := &Cache{
		dir:     dir,
		maxSize: config.MaxSize,
		entries: make(map[string]*entryMeta),
	}

	// 尝试加载已有索引
	if err := c.loadIndex(); err != nil {
		if !os.IsNotExist(err) {
			log.Warn("缓存：加载索引失败: %v，将使用空缓存", err)
		}
	}

	log.Info("缓存：已初始化 dir=%s max_size=%d entries=%d", dir, config.MaxSize, len(c.entries))
	return c, nil
}

// Put 写入缓存条目
// key: 缓存键（通常是 txID 或 CID）
// data: 要缓存的数据
func (c *Cache) Put(key string, data []byte) error {
	if key == "" {
		return fmt.Errorf("缓存键不能为空")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// 如果已存在，先删除旧条目
	if old, ok := c.entries[key]; ok {
		if err := c.removeEntryLocked(key, old); err != nil {
			log.Warn("缓存：删除旧条目失败 key=%s: %v", key, err)
		}
	}

	// 写入文件
	filePath := c.filePath(key)
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		return fmt.Errorf("创建缓存子目录失败: %w", err)
	}

	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("写入缓存文件失败: %w", err)
	}

	dataSize := int64(len(data))

	// 如果设置了大小限制，淘汰直到有足够空间
	if c.maxSize > 0 && c.currentSize+dataSize > c.maxSize {
		c.evictLocked(c.currentSize + dataSize - c.maxSize)
	}

	entry := &entryMeta{
		Key:        key,
		FilePath:    filePath,
		Size:       dataSize,
		LastAccess: time.Now(),
	}

	c.entries[key] = entry
	c.currentSize += dataSize
	c.totalEntries++
	c.dirty = true

	// 添加到 LRU 链表尾部
	c.lruPushBackLocked(key)

	log.Debug("缓存：写入 key=%s size=%d current_size=%d", key, dataSize, c.currentSize)

	// 异步保存索引
	go c.saveIndex()

	return nil
}

// Get 从缓存读取数据
// 返回 (data, found, error)
func (c *Cache) Get(key string) ([]byte, bool, error) {
	entry, ok := c.getEntry(key)
	if !ok {
		return nil, false, nil
	}

	data, err := os.ReadFile(entry.FilePath)
	if err != nil {
		if os.IsNotExist(err) {
			// 文件丢失，删除元数据
			c.mu.Lock()
			c.removeEntryLocked(key, entry)
			c.mu.Unlock()
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("读取缓存文件失败: %w", err)
	}

	return data, true, nil
}

// GetPath 获取缓存文件路径（不读取内容）
// 返回 (filePath, found, error)
func (c *Cache) GetPath(key string) (string, bool, error) {
	entry, ok := c.getEntry(key)
	if !ok {
		return "", false, nil
	}
	return entry.FilePath, true, nil
}

// Has 检查 key 是否存在
func (c *Cache) Has(key string) bool {
	_, ok := c.getEntry(key)
	return ok
}

// Delete 删除缓存条目
func (c *Cache) Delete(key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[key]
	if !ok {
		return nil
	}

	return c.removeEntryLocked(key, entry)
}

// Clear 清空所有缓存
func (c *Cache) Clear() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 删除所有文件
	for key, entry := range c.entries {
		if err := os.Remove(entry.FilePath); err != nil && !os.IsNotExist(err) {
			log.Warn("缓存：删除文件失败 %s: %v", entry.FilePath, err)
		}
		delete(c.entries, key)
	}

	// 删除索引文件
	indexPath := filepath.Join(c.dir, indexFileName)
	os.Remove(indexPath)

	c.currentSize = 0
	c.totalEntries = 0
	c.lruHead = nil
	c.lruTail = nil
	c.dirty = false

	log.Info("缓存：已清空")
	return nil
}

// Stats 返回缓存统计信息
func (c *Cache) Stats() CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return CacheStats{
		TotalEntries: c.totalEntries,
		CurrentSize:  c.currentSize,
		MaxSize:      c.maxSize,
		Hits:         c.hits,
		Misses:       c.misses,
		Evictions:    c.evictions,
	}
}

// Close 关闭缓存（刷新索引到磁盘）
func (c *Cache) Close() error {
	c.saveIndex()
	return nil
}

// getEntry 获取条目并更新 LRU
func (c *Cache) getEntry(key string) (*entryMeta, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[key]
	if !ok {
		c.misses++
		return nil, false
	}

	// 更新访问时间和 LRU
	entry.LastAccess = time.Now()
	c.lruMoveToBackLocked(key)
	c.hits++
	c.dirty = true

	return entry, true
}

// removeEntryLocked 删除条目（调用者必须持有 mu 写锁）
func (c *Cache) removeEntryLocked(key string, entry *entryMeta) error {
	if err := os.Remove(entry.FilePath); err != nil && !os.IsNotExist(err) {
		return err
	}

	delete(c.entries, key)
	c.currentSize -= entry.Size
	c.lruRemoveLocked(key)
	c.dirty = true

	return nil
}

// evictLocked 驱逐条目直到释放至少 targetBytes 空间
func (c *Cache) evictLocked(targetBytes int64) {
	var evictedSize int64
	for c.lruHead != nil && evictedSize < targetBytes {
		oldestKey := c.lruHead.key
		entry, ok := c.entries[oldestKey]
		if !ok {
			c.lruRemoveLocked(oldestKey)
			continue
		}

		if err := os.Remove(entry.FilePath); err != nil && !os.IsNotExist(err) {
			log.Warn("缓存：驱逐删除文件失败 %s: %v", entry.FilePath, err)
		}

		delete(c.entries, oldestKey)
		evictedSize += entry.Size
		c.currentSize -= entry.Size
		c.evictions++
		c.lruRemoveLocked(oldestKey)

		log.Debug("缓存：驱逐 key=%s size=%d evicted_total=%d", oldestKey, entry.Size, evictedSize)
	}
}

// filePath 生成缓存文件路径
func (c *Cache) filePath(key string) string {
	// 对 key 进行 sanitize，使用 key 的前缀分目录避免单目录文件过多
	safeKey := sanitizeKey(key)
	prefix := ""
	if len(safeKey) >= 4 {
		prefix = safeKey[:2] + "/" + safeKey[2:4]
	}
	return filepath.Join(c.dir, "data", prefix, safeKey)
}

// --- LRU 链表操作 ---

func (c *Cache) lruPushBackLocked(key string) {
	node := &lruNode{key: key}
	if c.lruTail == nil {
		c.lruHead = node
		c.lruTail = node
	} else {
		node.prev = c.lruTail
		c.lruTail.next = node
		c.lruTail = node
	}
}

func (c *Cache) lruMoveToBackLocked(key string) {
	c.lruRemoveLocked(key)
	c.lruPushBackLocked(key)
}

func (c *Cache) lruRemoveLocked(key string) {
	var node *lruNode
	for n := c.lruHead; n != nil; n = n.next {
		if n.key == key {
			node = n
			break
		}
	}
	if node == nil {
		return
	}

	if node.prev != nil {
		node.prev.next = node.next
	} else {
		c.lruHead = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	} else {
		c.lruTail = node.prev
	}
}

// --- 持久化 ---

// saveIndex 保存索引到磁盘
func (c *Cache) saveIndex() {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.dirty {
		return
	}

	entries := make([]*entryMeta, 0, len(c.entries))
	for _, e := range c.entries {
		entries = append(entries, e)
	}

	// 按 LastAccess 排序，方便加载时恢复 LRU 顺序
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].LastAccess.Before(entries[j].LastAccess)
	})

	data, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		log.Warn("缓存：序列化索引失败: %v", err)
		return
	}

	indexPath := filepath.Join(c.dir, indexFileName)
	tmpPath := indexPath + ".tmp"

	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		log.Warn("缓存：写入索引文件失败: %v", err)
		return
	}

	if err := os.Rename(tmpPath, indexPath); err != nil {
		log.Warn("缓存：重命名索引文件失败: %v", err)
		return
	}
}

// loadIndex 从磁盘加载索引
func (c *Cache) loadIndex() error {
	indexPath := filepath.Join(c.dir, indexFileName)
	data, err := os.ReadFile(indexPath)
	if err != nil {
		return err
	}

	var entries []*entryMeta
	if err := json.Unmarshal(data, &entries); err != nil {
		// 索引文件损坏，删除并重新开始
		os.Remove(indexPath)
		return fmt.Errorf("索引文件损坏: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, entry := range entries {
		// 验证文件是否存在
		if _, err := os.Stat(entry.FilePath); os.IsNotExist(err) {
			log.Debug("缓存：索引条目文件丢失，跳过 key=%s", entry.Key)
			continue
		}

		c.entries[entry.Key] = entry
		c.currentSize += entry.Size
		c.totalEntries++

		// 按 LastAccess 顺序重建 LRU 链表
		c.lruPushBackLocked(entry.Key)
	}

	log.Info("缓存：从磁盘加载了 %d 个条目，size=%d", len(c.entries), c.currentSize)
	return nil
}

// --- 工具函数 ---

// sanitizeKey 清理 key 中的非法文件名字符
func sanitizeKey(key string) string {
	var sb strings.Builder
	for _, c := range key {
		switch c {
		case '/', '\\', ':', '*', '?', '"', '<', '>', '|', ' ':
			sb.WriteByte('_')
		default:
			sb.WriteRune(c)
		}
	}
	return sb.String()
}

// CacheStats 缓存统计信息
type CacheStats struct {
	TotalEntries int64 `json:"total_entries"`
	CurrentSize  int64 `json:"current_size"`
	MaxSize      int64 `json:"max_size"`
	Hits         int64 `json:"hits"`
	Misses       int64 `json:"misses"`
	Evictions    int64 `json:"evictions"`
}
