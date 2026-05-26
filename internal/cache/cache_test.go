package cache

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestNewCache(t *testing.T) {
	tmpDir := t.TempDir()
	cacheDir := filepath.Join(tmpDir, "cache")

	c, err := New(CacheConfig{
		Dir:     cacheDir,
		MaxSize: 10 * 1024 * 1024, // 10 MB
	})
	if err != nil {
		t.Fatalf("New() 不应返回错误: %v", err)
	}
	defer c.Close()

	if c == nil {
		t.Fatal("New() 不应返回 nil")
	}

	// 验证目录已创建
	if _, err := os.Stat(cacheDir); os.IsNotExist(err) {
		t.Errorf("缓存目录应已创建: %s", cacheDir)
	}
}

func TestPutAndGet(t *testing.T) {
	tmpDir := t.TempDir()
	c, err := New(CacheConfig{
		Dir:     filepath.Join(tmpDir, "cache"),
		MaxSize: 10 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("New() 不应返回错误: %v", err)
	}
	defer c.Close()

	key := "test-key-1"
	data := []byte("hello, cache world!")

	err = c.Put(key, data)
	if err != nil {
		t.Fatalf("Put() 不应返回错误: %v", err)
	}

	got, ok, err := c.Get(key)
	if err != nil {
		t.Fatalf("Get() 不应返回错误: %v", err)
	}
	if !ok {
		t.Fatal("Get() 应找到 key")
	}
	if string(got) != string(data) {
		t.Errorf("Get() 数据不匹配: got %q, want %q", string(got), string(data))
	}
}

func TestGetNonExistent(t *testing.T) {
	tmpDir := t.TempDir()
	c, err := New(CacheConfig{
		Dir:     filepath.Join(tmpDir, "cache"),
		MaxSize: 10 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("New() 不应返回错误: %v", err)
	}
	defer c.Close()

	_, ok, err := c.Get("nonexistent")
	if err != nil {
		t.Fatalf("Get() 不应返回错误: %v", err)
	}
	if ok {
		t.Error("Get() 不应找到不存在的 key")
	}
}

func TestPutOverwrite(t *testing.T) {
	tmpDir := t.TempDir()
	c, err := New(CacheConfig{
		Dir:     filepath.Join(tmpDir, "cache"),
		MaxSize: 10 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("New() 不应返回错误: %v", err)
	}
	defer c.Close()

	key := "overwrite-key"
	data1 := []byte("version 1")
	data2 := []byte("version 2 - longer data")

	if err := c.Put(key, data1); err != nil {
		t.Fatalf("Put() v1 不应返回错误: %v", err)
	}
	if err := c.Put(key, data2); err != nil {
		t.Fatalf("Put() v2 不应返回错误: %v", err)
	}

	got, ok, err := c.Get(key)
	if err != nil {
		t.Fatalf("Get() 不应返回错误: %v", err)
	}
	if !ok {
		t.Fatal("Get() 应找到 key")
	}
	if string(got) != string(data2) {
		t.Errorf("Get() 应返回最新版本: got %q, want %q", string(got), string(data2))
	}
}

func TestDelete(t *testing.T) {
	tmpDir := t.TempDir()
	c, err := New(CacheConfig{
		Dir:     filepath.Join(tmpDir, "cache"),
		MaxSize: 10 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("New() 不应返回错误: %v", err)
	}
	defer c.Close()

	key := "delete-me"
	data := []byte("some data")

	if err := c.Put(key, data); err != nil {
		t.Fatalf("Put() 不应返回错误: %v", err)
	}

	if err := c.Delete(key); err != nil {
		t.Fatalf("Delete() 不应返回错误: %v", err)
	}

	_, ok, err := c.Get(key)
	if err != nil {
		t.Fatalf("Get() 不应返回错误: %v", err)
	}
	if ok {
		t.Error("Get() 不应找到已删除的 key")
	}
}

func TestLRUEviction(t *testing.T) {
	tmpDir := t.TempDir()
	maxSize := int64(1024) // 1 KB

	c, err := New(CacheConfig{
		Dir:     filepath.Join(tmpDir, "cache"),
		MaxSize: maxSize,
	})
	if err != nil {
		t.Fatalf("New() 不应返回错误: %v", err)
	}
	defer c.Close()

	// 写入多个 ~300 字节的条目，总大小会超过 1KB
	data := make([]byte, 300)
	for i := range data {
		data[i] = byte(i % 256)
	}

	// 写入 key1, key2, key3 (共 ~900 字节)
	if err := c.Put("key1", data); err != nil {
		t.Fatalf("Put key1 不应返回错误: %v", err)
	}
	if err := c.Put("key2", data); err != nil {
		t.Fatalf("Put key2 不应返回错误: %v", err)
	}
	if err := c.Put("key3", data); err != nil {
		t.Fatalf("Put key3 不应返回错误: %v", err)
	}

	// 确认当前大小不超过上限（稍微放宽，因为有元数据开销）
	stats := c.Stats()
	if stats.CurrentSize > maxSize+1024 {
		t.Errorf("CurrentSize %d 应接近 MaxSize %d", stats.CurrentSize, maxSize)
	}

	// 手动过期所有条目的 LastAccess，使驱逐可以发生
	// （因为 10 分钟保护期在单元测试中不现实）
	oldTime := time.Now().Add(-20 * time.Minute)
	c.mu.Lock()
	for _, entry := range c.entries {
		entry.LastAccess = oldTime
	}
	c.mu.Unlock()

	// 访问 key1 使其成为最近使用（同时刷新其 LastAccess）
	_, ok, _ := c.Get("key1")
	if !ok {
		t.Fatal("key1 应存在")
	}

	// 再次过期 key2 和 key3 的 LastAccess（key1 的 LastAccess 被 Get 刷新了）
	c.mu.Lock()
	if e, ok := c.entries["key2"]; ok {
		e.LastAccess = oldTime
	}
	if e, ok := c.entries["key3"]; ok {
		e.LastAccess = oldTime
	}
	c.mu.Unlock()

	// 写入 key4（300 字节），应触发淘汰 key2 或 key3（key1 受保护）
	if err := c.Put("key4", data); err != nil {
		t.Fatalf("Put key4 不应返回错误: %v", err)
	}

	// key1 应还在（被访问过 → 保护期刷新 → 不可驱逐）
	_, ok, _ = c.Get("key1")
	if !ok {
		t.Error("key1 应未被淘汰（最近访问过，仍在保护期内）")
	}

	// 至少有一个较早的 key 被淘汰
	_, ok2, _ := c.Get("key2")
	_, ok3, _ := c.Get("key3")
	if ok2 && ok3 {
		t.Error("key2 或 key3 应被淘汰")
	}

	stats = c.Stats()
	if stats.Evictions == 0 {
		t.Error("应有淘汰记录")
	}

	// 清理以便 TempDir 可以正常回收
	c.Clear()
}

func TestConcurrentAccess(t *testing.T) {
	tmpDir := t.TempDir()
	c, err := New(CacheConfig{
		Dir:     filepath.Join(tmpDir, "cache"),
		MaxSize: 50 * 1024 * 1024, // 50 MB
	})
	if err != nil {
		t.Fatalf("New() 不应返回错误: %v", err)
	}
	defer c.Close()

	var wg sync.WaitGroup
	numGoroutines := 20
	numOps := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				data := []byte(fmt.Sprintf("data from goroutine %d op %d", id, j))
				if err := c.Put(key, data); err != nil {
					t.Errorf("并发 Put 失败: %v", err)
					return
				}
				got, ok, err := c.Get(key)
				if err != nil {
					t.Errorf("并发 Get 失败: %v", err)
					return
				}
				if !ok {
					t.Errorf("并发 Get 应找到 key %s", key)
					return
				}
				if string(got) != string(data) {
					t.Errorf("并发数据不匹配: got %q, want %q", string(got), string(data))
					return
				}
			}
		}(i)
	}

	wg.Wait()

	stats := c.Stats()
	if stats.TotalEntries != int64(numGoroutines*numOps) {
		t.Errorf("TotalEntries 应为 %d，实际 %d", numGoroutines*numOps, stats.TotalEntries)
	}
}

func TestStats(t *testing.T) {
	tmpDir := t.TempDir()
	c, err := New(CacheConfig{
		Dir:     filepath.Join(tmpDir, "cache"),
		MaxSize: 10 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("New() 不应返回错误: %v", err)
	}
	defer c.Close()

	if err := c.Put("s1", []byte("aaa")); err != nil {
		t.Fatalf("Put s1 失败: %v", err)
	}
	if err := c.Put("s2", []byte("bbbbb")); err != nil {
		t.Fatalf("Put s2 失败: %v", err)
	}

	_, _, _ = c.Get("s1") // 记录一次命中
	_, _, _ = c.Get("s3") // 记录一次未命中

	stats := c.Stats()
	if stats.TotalEntries != 2 {
		t.Errorf("TotalEntries = %d, want 2", stats.TotalEntries)
	}
	if stats.Hits != 1 {
		t.Errorf("Hits = %d, want 1", stats.Hits)
	}
	if stats.Misses != 1 {
		t.Errorf("Misses = %d, want 1", stats.Misses)
	}
	if stats.CurrentSize <= 0 {
		t.Error("CurrentSize 应 > 0")
	}
}

func TestClear(t *testing.T) {
	tmpDir := t.TempDir()
	c, err := New(CacheConfig{
		Dir:     filepath.Join(tmpDir, "cache"),
		MaxSize: 10 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("New() 不应返回错误: %v", err)
	}
	defer c.Close()

	if err := c.Put("k1", []byte("v1")); err != nil {
		t.Fatalf("Put k1 失败: %v", err)
	}
	if err := c.Put("k2", []byte("v2")); err != nil {
		t.Fatalf("Put k2 失败: %v", err)
	}

	if err := c.Clear(); err != nil {
		t.Fatalf("Clear() 不应返回错误: %v", err)
	}

	_, ok, _ := c.Get("k1")
	if ok {
		t.Error("k1 应在 Clear 后被删除")
	}

	stats := c.Stats()
	if stats.TotalEntries != 0 {
		t.Errorf("TotalEntries 应为 0，实际 %d", stats.TotalEntries)
	}
	if stats.CurrentSize != 0 {
		t.Errorf("CurrentSize 应为 0，实际 %d", stats.CurrentSize)
	}
}

func TestPutCARFileAndGetPath(t *testing.T) {
	tmpDir := t.TempDir()
	c, err := New(CacheConfig{
		Dir:     filepath.Join(tmpDir, "cache"),
		MaxSize: 10 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("New() 不应返回错误: %v", err)
	}
	defer c.Close()

	key := "car-bafy123"
	data := []byte{0x1a, 0xa1, 0x12, 0x20} // 模拟 CAR 头部

	if err := c.Put(key, data); err != nil {
		t.Fatalf("Put() 不应返回错误: %v", err)
	}

	// 获取文件路径（不去读内容，只拿路径）
	path, ok, err := c.GetPath(key)
	if err != nil {
		t.Fatalf("GetPath() 不应返回错误: %v", err)
	}
	if !ok {
		t.Fatal("GetPath() 应找到 key")
	}
	if path == "" {
		t.Error("path 不应为空")
	}

	// 验证文件确实存在
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Errorf("缓存文件应存在: %s", path)
	}

	// 验证内容
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取缓存文件失败: %v", err)
	}
	if string(got) != string(data) {
		t.Error("缓存文件内容不匹配")
	}
}

func TestPutMetadataJSON(t *testing.T) {
	tmpDir := t.TempDir()
	c, err := New(CacheConfig{
		Dir:     filepath.Join(tmpDir, "cache"),
		MaxSize: 10 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("New() 不应返回错误: %v", err)
	}
	defer c.Close()

	key := "meta-txid-abc123"
	metaJSON := []byte(`{"version":1,"method":"raw","root_cid":"bafy123","data_txid":"tx456","data_height":100,"data_size":5000}`)

	if err := c.Put(key, metaJSON); err != nil {
		t.Fatalf("Put metadata 不应返回错误: %v", err)
	}

	got, ok, err := c.Get(key)
	if err != nil {
		t.Fatalf("Get metadata 不应返回错误: %v", err)
	}
	if !ok {
		t.Fatal("Get metadata 应找到 key")
	}
	if string(got) != string(metaJSON) {
		t.Error("metadata JSON 不匹配")
	}
}

func TestHas(t *testing.T) {
	tmpDir := t.TempDir()
	c, err := New(CacheConfig{
		Dir:     filepath.Join(tmpDir, "cache"),
		MaxSize: 10 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("New() 不应返回错误: %v", err)
	}
	defer c.Close()

	if c.Has("no-such-key") {
		t.Error("Has() 应对不存在的 key 返回 false")
	}

	if err := c.Put("exists", []byte("data")); err != nil {
		t.Fatalf("Put 失败: %v", err)
	}

	if !c.Has("exists") {
		t.Error("Has() 应对存在的 key 返回 true")
	}
}

func TestPersistenceAcrossRestarts(t *testing.T) {
	tmpDir := t.TempDir()
	cacheDir := filepath.Join(tmpDir, "cache")

	// 第一次创建并写入
	c1, err := New(CacheConfig{
		Dir:     cacheDir,
		MaxSize: 10 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("New() c1 不应返回错误: %v", err)
	}

	key := "persist-key"
	data := []byte("data that should persist")
	if err := c1.Put(key, data); err != nil {
		t.Fatalf("Put c1 失败: %v", err)
	}
	c1.Close()

	// 验证文件在磁盘上
	indexPath := filepath.Join(cacheDir, indexFileName)
	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		t.Errorf("索引文件应存在: %s", indexPath)
	}

	// 第二次打开
	c2, err := New(CacheConfig{
		Dir:     cacheDir,
		MaxSize: 10 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("New() c2 不应返回错误: %v", err)
	}

	got, ok, err := c2.Get(key)
	if err != nil {
		t.Fatalf("Get c2 不应返回错误: %v", err)
	}
	if !ok {
		t.Fatal("重启后应能找到持久化的 key")
	}
	if string(got) != string(data) {
		t.Errorf("持久化数据不匹配: got %q, want %q", string(got), string(data))
	}

	// 清理以便 TempDir 回收
	c2.Clear()
	c2.Close()
}

func TestMaxSizeZeroDisableEviction(t *testing.T) {
	tmpDir := t.TempDir()
	c, err := New(CacheConfig{
		Dir:     filepath.Join(tmpDir, "cache"),
		MaxSize: 0, // 无上限
	})
	if err != nil {
		t.Fatalf("New() 不应返回错误: %v", err)
	}
	defer c.Close()

	// 写入大量数据
	bigData := make([]byte, 100*1024) // 100 KB
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("big-%d", i)
		if err := c.Put(key, bigData); err != nil {
			t.Fatalf("Put big-%d 失败: %v", i, err)
		}
	}

	stats := c.Stats()
	if stats.Evictions != 0 {
		t.Errorf("MaxSize=0 时不应淘汰：实际淘汰 %d 次", stats.Evictions)
	}
	if stats.TotalEntries != 20 {
		t.Errorf("应有 20 个条目，实际 %d", stats.TotalEntries)
	}
}

func TestConfigDefaults(t *testing.T) {
	cfg := DefaultCacheConfig()
	if cfg.Dir != "cache/ipfar" {
		t.Errorf("默认 Dir 应为 cache/ipfar，实际 %s", cfg.Dir)
	}
	if cfg.MaxSize != 500*1024*1024 {
		t.Errorf("默认 MaxSize 应为 500 MB，实际 %d", cfg.MaxSize)
	}
}
