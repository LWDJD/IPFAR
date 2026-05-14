package bitswap

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	cid "github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"

	"github.com/lwdjd/IPFAR/internal/cache"
)

// makeTestCID 创建一个测试用的 CID
func makeTestCID(data string) cid.Cid {
	mh, _ := multihash.Sum([]byte(data), multihash.SHA2_256, -1)
	return cid.NewCidV1(cid.Raw, mh)
}

func TestNewService(t *testing.T) {
	tmpDir := t.TempDir()
	s, err := New(Config{
		ListenAddr:  "127.0.0.1:0",
		CacheConfig: cache.CacheConfig{Dir: tmpDir + "/cache", MaxSize: 10 * 1024 * 1024},
	})
	if err != nil {
		t.Fatalf("New() 不应返回错误: %v", err)
	}
	defer s.Close()

	if s.ListenAddr() == "" {
		t.Error("ListenAddr() 不应为空")
	}
}

func TestNewServiceWithoutListen(t *testing.T) {
	tmpDir := t.TempDir()
	s, err := New(Config{
		ListenAddr:  "", // 不监听
		CacheConfig: cache.CacheConfig{Dir: tmpDir + "/cache", MaxSize: 10 * 1024 * 1024},
	})
	if err != nil {
		t.Fatalf("New() 不应返回错误: %v", err)
	}
	defer s.Close()

	if s.ListenAddr() != "" {
		t.Errorf("不监听时 ListenAddr 应为空，得到 %s", s.ListenAddr())
	}
}

func TestPutAndHasBlock(t *testing.T) {
	tmpDir := t.TempDir()
	s, err := New(Config{
		ListenAddr:  "127.0.0.1:0",
		CacheConfig: cache.CacheConfig{Dir: tmpDir + "/cache", MaxSize: 10 * 1024 * 1024},
	})
	if err != nil {
		t.Fatalf("New() 不应返回错误: %v", err)
	}
	defer s.Close()

	c := makeTestCID("test-block-data")
	testData := []byte("hello bitswap!")

	// 直接写入缓存
	if err := s.Cache().Put(c.String(), testData); err != nil {
		t.Fatalf("Put to cache 失败: %v", err)
	}

	if !s.HasBlock(c) {
		t.Error("HasBlock 应对已缓存的 block 返回 true")
	}
}

func TestGetBlockFromCache(t *testing.T) {
	tmpDir := t.TempDir()
	s, err := New(Config{
		ListenAddr:  "127.0.0.1:0",
		CacheConfig: cache.CacheConfig{Dir: tmpDir + "/cache", MaxSize: 10 * 1024 * 1024},
	})
	if err != nil {
		t.Fatalf("New() 不应返回错误: %v", err)
	}
	defer s.Close()

	c := makeTestCID("block-in-cache")
	testData := []byte("cached data")

	// 写入缓存
	s.Cache().Put(c.String(), testData)

	// GetBlock 从缓存中获取（不经过网络）
	data, err := s.GetBlock(context.Background(), "", c)
	if err != nil {
		t.Fatalf("GetBlock 不应返回错误: %v", err)
	}
	if string(data) != string(testData) {
		t.Errorf("GetBlock 返回数据不匹配: got %q, want %q", string(data), string(testData))
	}
}

func TestRemoteGetBlock(t *testing.T) {
	// 启动服务端
	serverDir := t.TempDir()
	server, err := New(Config{
		ListenAddr:    "127.0.0.1:0",
		DelayedReply:  false, // 测试时不延迟
		DelayDuration: 0,
		Timeout:       5 * time.Second,
		CacheConfig:   cache.CacheConfig{Dir: serverDir + "/cache", MaxSize: 10 * 1024 * 1024},
	})
	if err != nil {
		t.Fatalf("创建 server 失败: %v", err)
	}
	defer server.Close()

	// 在服务端缓存一个 block
	c := makeTestCID("remote-block-data")
	testData := []byte("remote bitswap data from server")
	server.Cache().Put(c.String(), testData)

	// 启动客户端
	clientDir := t.TempDir()
	client, err := New(Config{
		ListenAddr:  "", // 客户端不监听
		Timeout:     5 * time.Second,
		CacheConfig: cache.CacheConfig{Dir: clientDir + "/cache", MaxSize: 10 * 1024 * 1024},
	})
	if err != nil {
		t.Fatalf("创建 client 失败: %v", err)
	}
	defer client.Close()

	// 客户端向服务端请求 block
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	data, err := client.GetBlock(ctx, server.ListenAddr(), c)
	if err != nil {
		t.Fatalf("client.GetBlock 失败: %v", err)
	}
	if string(data) != string(testData) {
		t.Errorf("client 收到数据不匹配: got %q, want %q", string(data), string(testData))
	}

	// 验证客户端也缓存了数据
	if !client.HasBlock(c) {
		t.Error("client 应在 GetBlock 后缓存数据")
	}
}

func TestDontHaveResponse(t *testing.T) {
	// 启动服务端
	serverDir := t.TempDir()
	server, err := New(Config{
		ListenAddr:    "127.0.0.1:0",
		DelayedReply:  false,
		DelayDuration: 0,
		Timeout:       5 * time.Second,
		CacheConfig:   cache.CacheConfig{Dir: serverDir + "/cache", MaxSize: 10 * 1024 * 1024},
	})
	if err != nil {
		t.Fatalf("创建 server 失败: %v", err)
	}
	defer server.Close()

	// 客户端
	clientDir := t.TempDir()
	client, err := New(Config{
		ListenAddr:  "",
		Timeout:     5 * time.Second,
		CacheConfig: cache.CacheConfig{Dir: clientDir + "/cache", MaxSize: 10 * 1024 * 1024},
	})
	if err != nil {
		t.Fatalf("创建 client 失败: %v", err)
	}
	defer client.Close()

	// 请求服务端没有的 block
	c := makeTestCID("nonexistent-block")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = client.GetBlock(ctx, server.ListenAddr(), c)
	if err == nil {
		t.Error("GetBlock 应对不存在 block 返回错误")
	}
}

func TestDelayedReply(t *testing.T) {
	// 服务端启用延迟回复
	serverDir := t.TempDir()
	server, err := New(Config{
		ListenAddr:    "127.0.0.1:0",
		DelayedReply:  true,
		DelayDuration: 200 * time.Millisecond,
		Timeout:       5 * time.Second,
		CacheConfig:   cache.CacheConfig{Dir: serverDir + "/cache", MaxSize: 10 * 1024 * 1024},
	})
	if err != nil {
		t.Fatalf("创建 server 失败: %v", err)
	}
	defer server.Close()

	// 缓存 block
	c := makeTestCID("delayed-block")
	server.Cache().Put(c.String(), []byte("delayed data"))

	// 客户端
	clientDir := t.TempDir()
	client, err := New(Config{
		ListenAddr:  "",
		Timeout:     5 * time.Second,
		CacheConfig: cache.CacheConfig{Dir: clientDir + "/cache", MaxSize: 10 * 1024 * 1024},
	})
	if err != nil {
		t.Fatalf("创建 client 失败: %v", err)
	}
	defer client.Close()

	start := time.Now()
	ctx := context.Background()
	data, err := client.GetBlock(ctx, server.ListenAddr(), c)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("GetBlock 失败: %v", err)
	}
	if string(data) != "delayed data" {
		t.Errorf("数据不匹配: got %q", string(data))
	}

	// 验证延迟生效（至少延迟了一半以上）
	if elapsed < server.config.DelayDuration/2 {
		t.Errorf("延迟回复应至少 %.0fms，实际 %.0fms",
			float64(server.config.DelayDuration)/float64(time.Millisecond),
			float64(elapsed)/float64(time.Millisecond))
	}
}

func TestConcurrentRequests(t *testing.T) {
	serverDir := t.TempDir()
	server, err := New(Config{
		ListenAddr:    "127.0.0.1:0",
		DelayedReply:  false,
		DelayDuration: 0,
		Timeout:       10 * time.Second,
		CacheConfig:   cache.CacheConfig{Dir: serverDir + "/cache", MaxSize: 50 * 1024 * 1024},
	})
	if err != nil {
		t.Fatalf("创建 server 失败: %v", err)
	}
	defer server.Close()

	// 缓存多个 block
	numBlocks := 20
	cids := make([]cid.Cid, numBlocks)
	expectedData := make(map[string]string)
	for i := 0; i < numBlocks; i++ {
		dataStr := fmt.Sprintf("concurrent-block-%d-data", i)
		c := makeTestCID(dataStr)
		cids[i] = c
		expectedData[c.String()] = dataStr
		server.Cache().Put(c.String(), []byte(dataStr))
	}

	// 多个客户端并发请求
	clientDir := t.TempDir()
	var wg sync.WaitGroup
	errs := make(chan error, numBlocks)

	for i := 0; i < numBlocks; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			client, err := New(Config{
				ListenAddr:  "",
				Timeout:     10 * time.Second,
				CacheConfig: cache.CacheConfig{Dir: clientDir + "/cache", MaxSize: 50 * 1024 * 1024},
			})
			if err != nil {
				errs <- err
				return
			}
			defer client.Close()

			data, err := client.GetBlock(context.Background(), server.ListenAddr(), cids[idx])
			if err != nil {
				errs <- fmt.Errorf("client %d: %w", idx, err)
				return
			}

			expected := expectedData[cids[idx].String()]
			if string(data) != expected {
				errs <- fmt.Errorf("client %d: data mismatch", idx)
				return
			}
		}(i)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}
}

func TestStats(t *testing.T) {
	tmpDir := t.TempDir()
	s, err := New(Config{
		ListenAddr:  "127.0.0.1:0",
		CacheConfig: cache.CacheConfig{Dir: tmpDir + "/cache", MaxSize: 10 * 1024 * 1024},
	})
	if err != nil {
		t.Fatalf("New() 不应返回错误: %v", err)
	}
	defer s.Close()

	c := makeTestCID("stats-block")
	s.Cache().Put(c.String(), []byte("stats data"))

	// 本地获取（不经过网络）
	_, _ = s.GetBlock(context.Background(), "", c)

	stats := s.Stats()
	if stats.BlocksRequested == 0 {
		t.Error("BlocksRequested 应 > 0")
	}
	if stats.CacheStats.CurrentSize <= 0 {
		t.Error("CacheStats.CurrentSize 应 > 0")
	}
}

func TestCloseRemovesListener(t *testing.T) {
	tmpDir := t.TempDir()
	s, err := New(Config{
		ListenAddr:  "127.0.0.1:0",
		CacheConfig: cache.CacheConfig{Dir: tmpDir + "/cache", MaxSize: 10 * 1024 * 1024},
	})
	if err != nil {
		t.Fatalf("New() 不应返回错误: %v", err)
	}

	addr := s.ListenAddr()
	s.Close()

	// 验证端口已释放
	conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
	if err == nil {
		conn.Close()
		t.Error("关闭后端口应不可达")
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.ListenAddr != ":4001" {
		t.Errorf("默认 ListenAddr 应为 :4001，实际 %s", cfg.ListenAddr)
	}
	if !cfg.DelayedReply {
		t.Error("默认 DelayedReply 应为 true")
	}
	if cfg.DelayDuration != 2*time.Second {
		t.Errorf("默认 DelayDuration 应为 2s，实际 %v", cfg.DelayDuration)
	}
}

func TestInvalidPeerAddr(t *testing.T) {
	tmpDir := t.TempDir()
	s, err := New(Config{
		ListenAddr:  "",
		Timeout:     1 * time.Second,
		CacheConfig: cache.CacheConfig{Dir: tmpDir + "/cache", MaxSize: 10 * 1024 * 1024},
	})
	if err != nil {
		t.Fatalf("New() 不应返回错误: %v", err)
	}
	defer s.Close()

	c := makeTestCID("no-peer")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err = s.GetBlock(ctx, "127.0.0.1:19999", c)
	if err == nil {
		t.Error("GetBlock 对无效地址应返回错误")
	}
}
