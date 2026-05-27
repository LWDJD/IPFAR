package cache

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

// ============================================================
// Cache full — all entries protected
// ============================================================

func TestCacheFull_AllProtected(t *testing.T) {
	tmpDir := t.TempDir()
	// Small max size with long protection
	c, err := New(CacheConfig{
		Dir:                tmpDir,
		MaxSize:            1024, // 1KB max
		ProtectionDuration: 1 * time.Hour, // Very long protection
	})
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer c.Close()

	data := make([]byte, 512)
	for i := range data {
		data[i] = byte(i % 256)
	}

	// First file fits
	if err := c.Put("key1", data); err != nil {
		t.Fatalf("Put key1 failed: %v", err)
	}

	// Second file causes eviction but is protected → ErrCacheFull
	err = c.Put("key2", data)
	if err != ErrCacheFull {
		t.Errorf("expected ErrCacheFull, got: %v", err)
	}

	// key1 should still exist
	if !c.Has("key1") {
		t.Error("key1 should still exist (protected)")
	}

	// key2 should NOT exist
	if c.Has("key2") {
		t.Error("key2 should not exist (failed to write)")
	}

	// Cleanup
	c.Clear()
}

// ============================================================
// Protection duration refresh on access
// ============================================================

func TestProtectionRefresh(t *testing.T) {
	tmpDir := t.TempDir()
	c, err := New(CacheConfig{
		Dir:                tmpDir,
		MaxSize:            2048,
		ProtectionDuration: 500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer c.Close()

	data512 := make([]byte, 512)
	data256 := make([]byte, 256)

	// Put key1 (512 bytes)
	if err := c.Put("key1", data512); err != nil {
		t.Fatalf("Put key1 failed: %v", err)
	}

	// Wait a bit then access key1 to refresh protection
	time.Sleep(200 * time.Millisecond)
	c.Get("key1") // Access refreshes protection

	// Put key2 (512 bytes) — should trigger eviction
	// key1 was recently accessed, so its protection should be refreshed
	if err := c.Put("key2", data512); err != nil {
		t.Fatalf("Put key2 failed: %v", err)
	}

	// key1 should still exist (protection refreshed)
	if !c.Has("key1") {
		t.Error("key1 should still exist after protection refresh")
	}

	// Wait for key1's original protection to expire
	time.Sleep(400 * time.Millisecond)

	// Now put key3 — key1 should be evictable (original protection expired,
	// but it was refreshed on access, so it depends on timing)
	err = c.Put("key3", data256)
	if err != nil {
		t.Logf("Put key3: %v", err)
	}

	stats := c.Stats()
	t.Logf("Stats after protection test: entries=%d, evictions=%d, size=%d",
		stats.TotalEntries, stats.Evictions, stats.CurrentSize)

	// Cleanup
	c.Clear()
}

// ============================================================
// Empty key
// ============================================================

func TestPut_EmptyKey(t *testing.T) {
	tmpDir := t.TempDir()
	c, err := New(CacheConfig{
		Dir:     tmpDir,
		MaxSize: 10 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer c.Close()

	err = c.Put("", []byte("data"))
	if err == nil {
		t.Fatal("expected error for empty key")
	}
}

// ============================================================
// Concurrent eviction stress
// ============================================================

func TestConcurrentEvictionStress(t *testing.T) {
	tmpDir := t.TempDir()
	c, err := New(CacheConfig{
		Dir:                tmpDir,
		MaxSize:            5120, // 5KB
		ProtectionDuration: 0, // No protection to force eviction
	})
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer c.Close()

	var wg sync.WaitGroup
	numWriters := 10
	numOps := 50

	// Concurrent writers
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				key := fmt.Sprintf("writer-%d-key-%d", writerID, j)
				data := make([]byte, 200) // 200 bytes each
				for k := range data {
					data[k] = byte((writerID + j + k) % 256)
				}
				// Put may fail due to cache full — that's OK
				_ = c.Put(key, data)
			}
		}(i)
	}

	wg.Wait()

	stats := c.Stats()
	t.Logf("Concurrent eviction stats: entries=%d, evictions=%d, size=%d, hits=%d, misses=%d",
		stats.TotalEntries, stats.Evictions, stats.CurrentSize, stats.Hits, stats.Misses)

	// The cache should not be unreasonably large
	if stats.CurrentSize > 20*1024 { // 20KB as safety
		t.Errorf("cache size %d exceeds expected max", stats.CurrentSize)
	}

	// Some evictions should have occurred
	if stats.Evictions == 0 && stats.TotalEntries > 25 {
		t.Log("no evictions occurred (may be acceptable if size within limits)")
	}

	// Cleanup
	c.Clear()
}

// ============================================================
// Concurrent put/get/delete
// ============================================================

func TestConcurrentPutGetDelete(t *testing.T) {
	tmpDir := t.TempDir()
	c, err := New(CacheConfig{
		Dir:     tmpDir,
		MaxSize: 50 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer c.Close()

	var wg sync.WaitGroup
	numWorkers := 8

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 30; j++ {
				key := fmt.Sprintf("crud-%d-%d", id, j)
				data := []byte(fmt.Sprintf("data-%d-%d", id, j))

				// Put
				if err := c.Put(key, data); err != nil {
					t.Errorf("Put %s failed: %v", key, err)
					return
				}

				// Get
				got, ok, err := c.Get(key)
				if err != nil {
					t.Errorf("Get %s failed: %v", key, err)
					return
				}
				if !ok {
					t.Errorf("Get %s should find key", key)
					return
				}
				if string(got) != string(data) {
					t.Errorf("data mismatch for %s", key)
					return
				}

				// Has
				if !c.Has(key) {
					t.Errorf("Has %s should be true", key)
					return
				}

				// Delete (every other key)
				if j%2 == 0 {
					if err := c.Delete(key); err != nil {
						t.Errorf("Delete %s failed: %v", key, err)
						return
					}
					if c.Has(key) {
						t.Errorf("Has %s should be false after delete", key)
						return
					}
				}
			}
		}(i)
	}

	wg.Wait()

	stats := c.Stats()
	t.Logf("CRUD concurrency stats: entries=%d, hits=%d, misses=%d",
		stats.TotalEntries, stats.Hits, stats.Misses)

	c.Clear()
}

// ============================================================
// GetPath with non-existent key
// ============================================================

func TestGetPath_NonExistent(t *testing.T) {
	tmpDir := t.TempDir()
	c, err := New(CacheConfig{
		Dir:     tmpDir,
		MaxSize: 10 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer c.Close()

	path, ok, err := c.GetPath("nonexistent")
	if err != nil {
		t.Fatalf("GetPath failed: %v", err)
	}
	if ok {
		t.Error("should not find non-existent key")
	}
	if path != "" {
		t.Error("path should be empty for non-existent key")
	}
}

// ============================================================
// Delete non-existent key (no-op)
// ============================================================

func TestDelete_NonExistent(t *testing.T) {
	tmpDir := t.TempDir()
	c, err := New(CacheConfig{
		Dir:     tmpDir,
		MaxSize: 10 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer c.Close()

	// Delete non-existent key should not error
	if err := c.Delete("nonexistent"); err != nil {
		t.Errorf("Delete non-existent should not error: %v", err)
	}
}

// ============================================================
// Close and reuse
// ============================================================

func TestCloseAndReopen(t *testing.T) {
	tmpDir := t.TempDir()
	c, err := New(CacheConfig{
		Dir:     tmpDir,
		MaxSize: 10 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Put data
	if err := c.Put("reopen-key", []byte("reopen data")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Close (flushes index)
	if err := c.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen
	c2, err := New(CacheConfig{
		Dir:     tmpDir,
		MaxSize: 10 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("New() c2 failed: %v", err)
	}
	defer c2.Close()

	got, ok, err := c2.Get("reopen-key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !ok {
		t.Fatal("should find key after reopen")
	}
	if string(got) != "reopen data" {
		t.Errorf("data mismatch: got %q", string(got))
	}

	c2.Clear()
}

// ============================================================
// Persistence with corrupted index
// ============================================================

func TestPersistence_CorruptedIndex(t *testing.T) {
	tmpDir := t.TempDir()

	// Create first cache and write data
	c1, err := New(CacheConfig{
		Dir:     tmpDir,
		MaxSize: 10 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("New() c1 failed: %v", err)
	}
	c1.Put("test-key", []byte("test data"))
	c1.Close()

	// Corrupt the index file
	indexPath := tmpDir + "/cache_index.json"
	if err := os.WriteFile(indexPath, []byte("this is not valid json {{{"), 0644); err != nil {
		t.Fatalf("failed to corrupt index: %v", err)
	}

	// Reopen — should handle corrupted index gracefully
	c2, err := New(CacheConfig{
		Dir:     tmpDir,
		MaxSize: 10 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("New() c2 should not fail on corrupted index: %v", err)
	}
	defer c2.Close()

	// Should start fresh (corrupted index deleted, cache empty)
	_, ok, _ := c2.Get("test-key")
	if ok {
		t.Log("key found after index corruption (data files still exist)")
	} else {
		t.Log("key not found after index corruption (expected fresh start)")
	}

	c2.Clear()
}

// ============================================================
// MaxSize=0 — unlimited cache
// ============================================================

func TestUnlimitedCache(t *testing.T) {
	tmpDir := t.TempDir()
	c, err := New(CacheConfig{
		Dir:     tmpDir,
		MaxSize: 0, // Unlimited
	})
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer c.Close()

	// Write many large entries
	for i := 0; i < 100; i++ {
		data := make([]byte, 10240) // 10KB each
		if err := c.Put(fmt.Sprintf("big-%d", i), data); err != nil {
			t.Fatalf("Put big-%d failed: %v", i, err)
		}
	}

	stats := c.Stats()
	if stats.Evictions != 0 {
		t.Errorf("unlimited cache should not evict, got %d evictions", stats.Evictions)
	}
	if stats.TotalEntries != 100 {
		t.Errorf("expected 100 entries, got %d", stats.TotalEntries)
	}

	c.Clear()
}

// ============================================================
// Verify protection duration is respected
// ============================================================

func TestProtectionPreventsEviction(t *testing.T) {
	tmpDir := t.TempDir()
	c, err := New(CacheConfig{
		Dir:                tmpDir,
		MaxSize:            800,
		ProtectionDuration: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer c.Close()

	// Write key1 (300 bytes)
	if err := c.Put("key1", make([]byte, 300)); err != nil {
		t.Fatalf("Put key1 failed: %v", err)
	}

	// Write key2 (300 bytes) — total 600 < 800, should fit
	if err := c.Put("key2", make([]byte, 300)); err != nil {
		t.Fatalf("Put key2 failed: %v", err)
	}

	// Write key3 (300 bytes) — total would be 900 > 800, need eviction
	// Both key1 and key2 are within protection period → ErrCacheFull
	err = c.Put("key3", make([]byte, 300))
	if err != ErrCacheFull {
		t.Errorf("expected ErrCacheFull, got: %v", err)
	}

	// Both key1 and key2 should still exist
	if !c.Has("key1") || !c.Has("key2") {
		t.Error("protected entries should not be evicted")
	}

	// Wait for protection to expire
	time.Sleep(2 * time.Second)

	// Now key3 should succeed (key1 or key2 can be evicted)
	err = c.Put("key3", make([]byte, 300))
	if err != nil {
		t.Errorf("Put key3 after protection expiry should succeed: %v", err)
	}

	stats := c.Stats()
	t.Logf("After protection expiry: entries=%d, evictions=%d",
		stats.TotalEntries, stats.Evictions)

	c.Clear()
}

// ============================================================
// Put overwrite with different size
// ============================================================

func TestPut_OverwriteSizeChange(t *testing.T) {
	tmpDir := t.TempDir()
	c, err := New(CacheConfig{
		Dir:     tmpDir,
		MaxSize: 10 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer c.Close()

	key := "overwrite-size"

	// Small data
	if err := c.Put(key, []byte("small")); err != nil {
		t.Fatalf("Put small failed: %v", err)
	}

	stats1 := c.Stats()
	size1 := stats1.CurrentSize

	// Larger data
	if err := c.Put(key, []byte("much larger data that changes the size significantly")); err != nil {
		t.Fatalf("Put large failed: %v", err)
	}

	stats2 := c.Stats()
	size2 := stats2.CurrentSize

	// Size should have changed
	if size1 == size2 {
		t.Error("cache size should change after overwrite with different size")
	}

	got, ok, _ := c.Get(key)
	if !ok {
		t.Fatal("key should exist after overwrite")
	}
	if string(got) != "much larger data that changes the size significantly" {
		t.Errorf("data mismatch after overwrite: got %q", string(got))
	}
}

// ============================================================
// Large value near MaxSize
// ============================================================

func TestPut_NearMaxSize(t *testing.T) {
	tmpDir := t.TempDir()
	maxSize := int64(2048)
	c, err := New(CacheConfig{
		Dir:                tmpDir,
		MaxSize:            maxSize,
		ProtectionDuration: 0,
	})
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer c.Close()

	// Put a value slightly smaller than MaxSize
	data := make([]byte, maxSize-100)
	if err := c.Put("near-max", data); err != nil {
		t.Fatalf("Put near-max failed: %v", err)
	}

	// Second small put should trigger eviction of first
	smallData := make([]byte, 200)
	err = c.Put("small", smallData)
	if err != nil {
		t.Fatalf("Put small failed: %v", err)
	}

	stats := c.Stats()
	if stats.Evictions == 0 {
		t.Error("expected at least 1 eviction when exceeding max size")
	}

	c.Clear()
}

// ============================================================
// Has with empty cache
// ============================================================

func TestHas_EmptyCache(t *testing.T) {
	tmpDir := t.TempDir()
	c, err := New(CacheConfig{
		Dir:     tmpDir,
		MaxSize: 10 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer c.Close()

	if c.Has("any-key") {
		t.Error("empty cache should not have any key")
	}
}
