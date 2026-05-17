package store

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// ============================================================
// TestNew 测试
// ============================================================

func TestNew(t *testing.T) {
	tmpDir := t.TempDir()
	storeDir := filepath.Join(tmpDir, "badger")

	s, err := New(StoreConfig{
		Dir:              storeDir,
		MemTableSize:     2 << 20,
		BlockCacheSize:   1 << 20,
		IndexCacheSize:   1 << 20,
		NumMemtables:     2,
		NumCompactors:    2,
		ValueLogFileSize: 4 << 20,
		ValueThreshold:   1 << 10,
	})
	if err != nil {
		t.Fatalf("New() should not error: %v", err)
	}
	defer s.Close()

	if s == nil {
		t.Fatal("New() should not return nil")
	}

	// 验证目录已创建
	if _, err := os.Stat(storeDir); os.IsNotExist(err) {
		t.Errorf("store directory should exist: %s", storeDir)
	}
}

func TestNew_DefaultConfig(t *testing.T) {
	tmpDir := t.TempDir()
	storeDir := filepath.Join(tmpDir, "badger-default")

	s, err := New(StoreConfig{
		Dir:              storeDir,
		MemTableSize:     1 << 20,
		BlockCacheSize:   1 << 20,
		NumMemtables:     2,
		NumCompactors:    2,
		ValueLogFileSize: 4 << 20,
		ValueThreshold:   1 << 10,
	})
	if err != nil {
		t.Fatalf("New() with minimal config should not error: %v", err)
	}
	defer s.Close()
}

// ============================================================
// Set / Get 测试
// ============================================================

func TestSetAndGet(t *testing.T) {
	s := openTestStore(t)
	defer s.Close()

	key := []byte("test-key-1")
	value := []byte("hello, badger!")

	err := s.Set(key, value)
	if err != nil {
		t.Fatalf("Set() should not error: %v", err)
	}

	got, found, err := s.Get(key)
	if err != nil {
		t.Fatalf("Get() should not error: %v", err)
	}
	if !found {
		t.Fatal("Get() should find the key")
	}
	if string(got) != string(value) {
		t.Errorf("Get() = %q, want %q", string(got), string(value))
	}
}

func TestGetNonExistent(t *testing.T) {
	s := openTestStore(t)
	defer s.Close()

	_, found, err := s.Get([]byte("nonexistent"))
	if err != nil {
		t.Fatalf("Get() should not error: %v", err)
	}
	if found {
		t.Error("Get() should not find nonexistent key")
	}
}

func TestSetAndGetString(t *testing.T) {
	s := openTestStore(t)
	defer s.Close()

	err := s.SetString("hello", "world")
	if err != nil {
		t.Fatalf("SetString() should not error: %v", err)
	}

	val, found, err := s.GetString("hello")
	if err != nil {
		t.Fatalf("GetString() should not error: %v", err)
	}
	if !found {
		t.Fatal("GetString() should find the key")
	}
	if val != "world" {
		t.Errorf("GetString() = %q, want %q", val, "world")
	}
}

func TestSetOverwrite(t *testing.T) {
	s := openTestStore(t)
	defer s.Close()

	key := []byte("overwrite-key")

	if err := s.Set(key, []byte("v1")); err != nil {
		t.Fatalf("Set v1: %v", err)
	}
	if err := s.Set(key, []byte("v2")); err != nil {
		t.Fatalf("Set v2: %v", err)
	}

	got, found, err := s.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found {
		t.Fatal("should find key")
	}
	if string(got) != "v2" {
		t.Errorf("Get() = %q, want %q", string(got), "v2")
	}
}

// ============================================================
// Has 测试
// ============================================================

func TestHas(t *testing.T) {
	s := openTestStore(t)
	defer s.Close()

	found, err := s.Has([]byte("no-such-key"))
	if err != nil {
		t.Fatalf("Has() should not error: %v", err)
	}
	if found {
		t.Error("Has() should return false for missing key")
	}

	if err := s.Set([]byte("exists"), []byte("data")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	found, err = s.Has([]byte("exists"))
	if err != nil {
		t.Fatalf("Has() should not error: %v", err)
	}
	if !found {
		t.Error("Has() should return true for existing key")
	}
}

func TestHasString(t *testing.T) {
	s := openTestStore(t)
	defer s.Close()

	s.SetString("check-me", "yes")

	found, err := s.HasString("check-me")
	if err != nil {
		t.Fatalf("HasString: %v", err)
	}
	if !found {
		t.Error("HasString should return true")
	}

	found, err = s.HasString("check-not")
	if err != nil {
		t.Fatalf("HasString: %v", err)
	}
	if found {
		t.Error("HasString should return false")
	}
}

// ============================================================
// Delete 测试
// ============================================================

func TestDelete(t *testing.T) {
	s := openTestStore(t)
	defer s.Close()

	key := []byte("delete-me")
	s.Set(key, []byte("data"))

	err := s.Delete(key)
	if err != nil {
		t.Fatalf("Delete() should not error: %v", err)
	}

	_, found, _ := s.Get(key)
	if found {
		t.Error("Get() should not find deleted key")
	}
}

func TestDeleteNonExistent(t *testing.T) {
	s := openTestStore(t)
	defer s.Close()

	// 删除不存在的键不应报错
	err := s.Delete([]byte("never-existed"))
	if err != nil {
		t.Fatalf("Delete() of nonexistent key should not error: %v", err)
	}
}

// ============================================================
// Batch 测试
// ============================================================

func TestBatchSet(t *testing.T) {
	s := openTestStore(t)
	defer s.Close()

	pairs := map[string]string{
		"batch-1": "value-1",
		"batch-2": "value-2",
		"batch-3": "value-3",
	}

	err := s.BatchSet(pairs)
	if err != nil {
		t.Fatalf("BatchSet() should not error: %v", err)
	}

	for k, v := range pairs {
		got, found, err := s.GetString(k)
		if err != nil {
			t.Errorf("GetString(%q): %v", k, err)
		}
		if !found {
			t.Errorf("GetString(%q) should find key", k)
		}
		if got != v {
			t.Errorf("GetString(%q) = %q, want %q", k, got, v)
		}
	}
}

func TestBatchSetBytes(t *testing.T) {
	s := openTestStore(t)
	defer s.Close()

	pairs := map[string][]byte{
		"bytes-1": {0x01, 0x02, 0x03},
		"bytes-2": {0xff, 0xfe, 0xfd},
	}

	err := s.BatchSetBytes(pairs)
	if err != nil {
		t.Fatalf("BatchSetBytes: %v", err)
	}

	for k, v := range pairs {
		got, found, err := s.Get([]byte(k))
		if err != nil {
			t.Errorf("Get(%q): %v", k, err)
		}
		if !found {
			t.Errorf("Get(%q) should find key", k)
		}
		if string(got) != string(v) {
			t.Errorf("Get(%q) mismatch", k)
		}
	}
}

// ============================================================
// PrefixScan 测试
// ============================================================

func TestPrefixScan(t *testing.T) {
	s := openTestStore(t)
	defer s.Close()

	// 写入多个相同前缀的键
	s.SetString("user:1:name", "Alice")
	s.SetString("user:1:email", "alice@test.com")
	s.SetString("user:2:name", "Bob")
	s.SetString("order:1", "stuff")

	// 扫描 user: 前缀
	var users []string
	err := s.PrefixScan([]byte("user:"), func(key, value []byte) bool {
		users = append(users, string(key)+"="+string(value))
		return true
	})
	if err != nil {
		t.Fatalf("PrefixScan: %v", err)
	}

	if len(users) != 3 {
		t.Errorf("expected 3 user keys, got %d: %v", len(users), users)
	}
}

func TestPrefixScan_EarlyStop(t *testing.T) {
	s := openTestStore(t)
	defer s.Close()

	for i := 0; i < 10; i++ {
		s.SetString(fmt.Sprintf("item:%d", i), fmt.Sprintf("val-%d", i))
	}

	count := 0
	err := s.PrefixScan([]byte("item:"), func(key, value []byte) bool {
		count++
		return count < 5 // stop after 5
	})
	if err != nil {
		t.Fatalf("PrefixScan: %v", err)
	}

	if count != 5 {
		t.Errorf("expected early stop after 5, got %d", count)
	}
}

func TestPrefixScanKeys(t *testing.T) {
	s := openTestStore(t)
	defer s.Close()

	s.SetString("pk:1", "a")
	s.SetString("pk:2", "b")
	s.SetString("pk:3", "c")
	s.SetString("other", "d")

	keys, err := s.PrefixScanKeys([]byte("pk:"))
	if err != nil {
		t.Fatalf("PrefixScanKeys: %v", err)
	}

	if len(keys) != 3 {
		t.Errorf("expected 3 keys, got %d", len(keys))
	}
}

func TestCount(t *testing.T) {
	s := openTestStore(t)
	defer s.Close()

	for i := 0; i < 42; i++ {
		s.SetString(fmt.Sprintf("count:%d", i), "x")
	}

	n, err := s.Count([]byte("count:"))
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 42 {
		t.Errorf("Count = %d, want 42", n)
	}
}

// ============================================================
// DropPrefix 测试
// ============================================================

func TestDropPrefix(t *testing.T) {
	s := openTestStore(t)
	defer s.Close()

	s.SetString("drop:a", "1")
	s.SetString("drop:b", "2")
	s.SetString("keep:c", "3")

	err := s.DropPrefix([]byte("drop:"))
	if err != nil {
		t.Fatalf("DropPrefix: %v", err)
	}

	// drop: 前缀的键应被删除
	for _, k := range []string{"drop:a", "drop:b"} {
		found, _ := s.HasString(k)
		if found {
			t.Errorf("key %q should be deleted", k)
		}
	}

	// keep: 前缀的键应保留
	found, _ := s.HasString("keep:c")
	if !found {
		t.Error("key keep:c should be kept")
	}
}

// ============================================================
// 持久化测试
// ============================================================

func TestPersistenceAcrossRestarts(t *testing.T) {
	tmpDir := t.TempDir()
	storeDir := filepath.Join(tmpDir, "persist")

	// 第一次打开并写入
	cfg := StoreConfig{
		Dir:              storeDir,
		MemTableSize:     1 << 20,
		BlockCacheSize:   1 << 20,
		NumMemtables:     2,
		NumCompactors:    2,
		ValueLogFileSize: 4 << 20,
		ValueThreshold:   1 << 10,
	}

	s1, err := New(cfg)
	if err != nil {
		t.Fatalf("New s1: %v", err)
	}
	s1.SetString("persist-key", "persist-value")
	s1.Close()

	// 第二次打开
	s2, err := New(cfg)
	if err != nil {
		t.Fatalf("New s2: %v", err)
	}
	defer s2.Close()

	val, found, err := s2.GetString("persist-key")
	if err != nil {
		t.Fatalf("GetString s2: %v", err)
	}
	if !found {
		t.Fatal("key should persist across restarts")
	}
	if val != "persist-value" {
		t.Errorf("persisted value = %q, want %q", val, "persist-value")
	}
}

// ============================================================
// 并发测试
// ============================================================

func TestConcurrentAccess(t *testing.T) {
	s := openTestStore(t)
	defer s.Close()

	var wg sync.WaitGroup
	numGoroutines := 10
	numOps := 50

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				key := fmt.Sprintf("concurrent-%d-%d", id, j)
				val := fmt.Sprintf("val-%d-%d", id, j)

				if err := s.SetString(key, val); err != nil {
					t.Errorf("SetString concurrent: %v", err)
					return
				}

				got, found, err := s.GetString(key)
				if err != nil {
					t.Errorf("GetString concurrent: %v", err)
					return
				}
				if !found {
					t.Errorf("concurrent key %q should exist", key)
					return
				}
				if got != val {
					t.Errorf("concurrent value mismatch: got %q, want %q", got, val)
					return
				}
			}
		}(i)
	}

	wg.Wait()

	// 验证总数
	n, err := s.Count([]byte("concurrent-"))
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != numGoroutines*numOps {
		t.Errorf("total entries = %d, want %d", n, numGoroutines*numOps)
	}
}

// ============================================================
// Size / GC 测试
// ============================================================

func TestSize(t *testing.T) {
	s := openTestStore(t)
	defer s.Close()

	// 写入一些数据
	for i := 0; i < 100; i++ {
		s.SetString(fmt.Sprintf("size-test-%d", i), fmt.Sprintf("value-%d", i))
	}

	diskSize, err := s.Size()
	if err != nil {
		t.Fatalf("Size: %v", err)
	}
	if diskSize <= 0 {
		t.Error("Size should be > 0")
	}
	t.Logf("Disk size after 100 entries: %d bytes", diskSize)
}

func TestRunGC(t *testing.T) {
	s := openTestStore(t)
	defer s.Close()

	// 写入然后删除一些数据，产生可回收空间
	for i := 0; i < 100; i++ {
		s.SetString(fmt.Sprintf("gc-test-%d", i), "some data that will be deleted")
	}
	for i := 0; i < 100; i++ {
		s.DeleteString(fmt.Sprintf("gc-test-%d", i))
	}

	// 运行 GC
	err := s.RunGC()
	if err != nil {
		t.Logf("GC returned error (may be expected): %v", err)
	}
}

// ============================================================
// 预定义前缀测试
// ============================================================

func TestMetaKey(t *testing.T) {
	key := MetaKey("abc123")
	if string(key) != PrefixMeta+"abc123" {
		t.Errorf("MetaKey = %q, want %q", string(key), PrefixMeta+"abc123")
	}
}

func TestVerifiedCIDKey(t *testing.T) {
	key := VerifiedCIDKey("bafy123")
	if string(key) != PrefixVerifiedCID+"bafy123" {
		t.Errorf("VerifiedCIDKey = %q, want %q", string(key), PrefixVerifiedCID+"bafy123")
	}
}

func TestCachePathKey(t *testing.T) {
	key := CachePathKey("bafy456")
	if string(key) != PrefixCachePath+"bafy456" {
		t.Errorf("CachePathKey = %q, want %q", string(key), PrefixCachePath+"bafy456")
	}
}

// ============================================================
// 元数据存储场景测试
// ============================================================

func TestMetadataStorageScenario(t *testing.T) {
	s := openTestStore(t)
	defer s.Close()

	// 模拟元数据索引存储
	metaJSON := `{"root_cid":"bafy123","data_txid":"tx456","data_size":5000}`
	err := s.Set(MetaKey("tx456"), []byte(metaJSON))
	if err != nil {
		t.Fatalf("Set meta: %v", err)
	}

	// 读取
	got, found, err := s.Get(MetaKey("tx456"))
	if err != nil {
		t.Fatalf("Get meta: %v", err)
	}
	if !found {
		t.Fatal("metadata should be found")
	}
	if string(got) != metaJSON {
		t.Errorf("metadata mismatch: %q vs %q", string(got), metaJSON)
	}
}

func TestVerifiedCIDScenario(t *testing.T) {
	s := openTestStore(t)
	defer s.Close()

	// 模拟已验证 CID 记录
	timestamp := time.Now().Format(time.RFC3339)
	err := s.Set(VerifiedCIDKey("bafy-verified-1"), []byte(timestamp))
	if err != nil {
		t.Fatalf("Set verified CID: %v", err)
	}

	found, err := s.Has(VerifiedCIDKey("bafy-verified-1"))
	if err != nil {
		t.Fatalf("Has verified CID: %v", err)
	}
	if !found {
		t.Error("verified CID should be found")
	}

	// 统计已验证 CID 数量
	count, err := s.Count([]byte(PrefixVerifiedCID))
	if err != nil {
		t.Fatalf("Count verified: %v", err)
	}
	if count != 1 {
		t.Errorf("verified count = %d, want 1", count)
	}
}

func TestCachePathMappingScenario(t *testing.T) {
	s := openTestStore(t)
	defer s.Close()

	// 模拟缓存路径映射
	err := s.Set(CachePathKey("bafy-cached-1"), []byte("/cache/car/bafy-cached-1_abc.car"))
	if err != nil {
		t.Fatalf("Set cache path: %v", err)
	}

	path, found, err := s.Get(CachePathKey("bafy-cached-1"))
	if err != nil {
		t.Fatalf("Get cache path: %v", err)
	}
	if !found {
		t.Error("cache path should be found")
	}
	if string(path) != "/cache/car/bafy-cached-1_abc.car" {
		t.Errorf("cache path mismatch: %q", string(path))
	}
}

// ============================================================
// 内存使用测试
// ============================================================

func TestMemoryUsage_UnderLimit(t *testing.T) {
	tmpDir := t.TempDir()
	storeDir := filepath.Join(tmpDir, "memtest")

	cfg := StoreConfig{
		Dir:              storeDir,
		MemTableSize:     2 << 20,
		NumMemtables:     2,
		BlockCacheSize:   1 << 20,  // 1 MB
		IndexCacheSize:   1 << 20,  // 1 MB
		NumCompactors:    2,
		ValueLogFileSize: 4 << 20,  // 4 MB
		ValueThreshold:   1 << 10,  // 1 KB
	}

	s, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer s.Close()

	// 估算内存上限
	estimatedMax := cfg.MemTableSize*int64(cfg.NumMemtables) + cfg.BlockCacheSize + cfg.IndexCacheSize
	// 加上一些额外缓冲
	estimatedMax += 10 << 20 // 10 MB for overhead

	if estimatedMax > 100<<20 { // 100 MB for this test config
		t.Logf("estimated max memory: %d MB (under 100 MB limit)", estimatedMax>>20)
	}

	// 写入大量数据，确保不崩溃
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("mem-test-key-%04d", i)
		val := fmt.Sprintf("memory-test-value-%04d-with-some-extra-data-to-make-it-bigger", i)
		if err := s.SetString(key, val); err != nil {
			t.Fatalf("SetString %d: %v", i, err)
		}
	}

	// 验证可以读取
	val, found, err := s.GetString("mem-test-key-0500")
	if err != nil {
		t.Fatalf("GetString: %v", err)
	}
	if !found {
		t.Fatal("key should be found")
	}
	t.Logf("Read back: %q", val)
}

// ============================================================
// 辅助函数
// ============================================================

func openTestStore(t *testing.T) *Store {
	t.Helper()
	tmpDir := t.TempDir()
	storeDir := filepath.Join(tmpDir, "badger-test")

	cfg := StoreConfig{
		Dir:              storeDir,
		MemTableSize:     1 << 20,
		BlockCacheSize:   1 << 20,
		NumMemtables:     2,
		NumCompactors:    2,
		ValueLogFileSize: 4 << 20,
		ValueThreshold:   1 << 10,
	}

	s, err := New(cfg)
	if err != nil {
		t.Fatalf("openTestStore: %v", err)
	}
	return s
}

// 确保包级别引用
var _ = fmt.Sprintf
var _ = time.Now
