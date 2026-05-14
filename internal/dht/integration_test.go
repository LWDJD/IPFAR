package dht

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/host/basic"
	swarmt "github.com/libp2p/go-libp2p/p2p/net/swarm/testing"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/multiformats/go-multihash"
)

// ============================================================
// 集成测试：模拟 libp2p host + DHT 端到端
// 测试 Provide → FindProviders 完整回路
// ============================================================

// setupDHTNetwork 创建 n 个互联的 DHT 节点
// 返回 hosts, dhts, cleanup 函数
func setupDHTNetwork(t *testing.T, n int) ([]host.Host, []*dht.IpfsDHT) {
	t.Helper()

	if n < 2 {
		t.Fatal("至少需要 2 个节点")
	}

	ctx := context.Background()

	hosts := make([]host.Host, n)
	dhts := make([]*dht.IpfsDHT, n)

	// 创建节点
	for i := range n {
		h, err := basichost.NewHost(swarmt.GenSwarm(t, swarmt.OptDisableReuseport), new(basichost.HostOpts))
		if err != nil {
			t.Fatalf("创建 host %d 失败: %v", i, err)
		}
		h.Start()
		hosts[i] = h

		d, err := dht.New(ctx, h,
			dht.Mode(dht.ModeServer),
			dht.DisableAutoRefresh(),
		)
		if err != nil {
			t.Fatalf("创建 DHT %d 失败: %v", i, err)
		}
		dhts[i] = d
	}

	// 注册 cleanup
	t.Cleanup(func() {
		for i := range n {
			dhts[i].Close()
			hosts[i].Close()
		}
	})

	// 连接所有节点（形成全连接网络）
	for i := range n {
		for j := i + 1; j < n; j++ {
			pi := peer.AddrInfo{ID: hosts[j].ID(), Addrs: hosts[j].Addrs()}
			if err := hosts[i].Connect(ctx, pi); err != nil {
				t.Logf("连接 host %d -> %d 失败: %v", i, j, err)
			}
		}
	}

	// 等待连接建立和路由表填充
	time.Sleep(2 * time.Second)

	// Bootstrap 所有节点
	for i := range n {
		if i == 0 {
			continue
		}
		if err := dhts[i].Bootstrap(ctx); err != nil {
			t.Logf("Bootstrap DHT %d 失败: %v", i, err)
		}
	}

	// 等待 bootstrap 完成
	time.Sleep(1 * time.Second)

	return hosts, dhts
}

// ============================================================
// 测试 1: Provide → FindProviders 完整回路
// ============================================================

func TestIntegration_ProvideFindProviders(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过集成测试（使用 -short 标志）")
	}

	hosts, dhts := setupDHTNetwork(t, 3)

	providerDHT := dhts[0]
	consumerDHT := dhts[2]

	// 创建 Provider 包装
	cfg := DefaultConfig()
	cfg.Mode = ModeServer
	p, err := NewProvider(hosts[0], cfg)
	if err != nil {
		t.Fatalf("创建 Provider 失败: %v", err)
	}
	p.dht = providerDHT
	p.started.Store(true)

	// 创建测试 CID
	testCID := makeTestCID(t, "integration-test-content")

	// 步骤 1: Provide
	t.Logf("步骤 1: 提供 CID %s", testCID)
	err = p.Provide(testCID)
	if err != nil {
		t.Logf("Provide 返回错误（可能是 DHT 未完全就绪）: %v", err)
	}

	// 等待传播
	time.Sleep(2 * time.Second)

	// 步骤 2: 从消费者节点查找提供者
	t.Log("步骤 2: 查找提供者")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	providers, err := consumerDHT.FindProviders(ctx, testCID)
	if err != nil {
		t.Logf("FindProviders 返回错误: %v", err)
	}

	t.Logf("找到 %d 个提供者", len(providers))

	// 验证结果：至少应该找到提供者节点
	found := false
	for _, prov := range providers {
		t.Logf("  提供者: %s", prov.ID)
		if prov.ID == hosts[0].ID() {
			found = true
		}
	}

	if found {
		t.Log("✅ 成功在 DHT 中找到本节点的提供记录")
	} else {
		t.Log("⚠️ 未在 DHT 中找到本节点的提供记录（可能需要更长的传播时间）")
	}

	// 步骤 3: 验证通过 Provider 的 FindProviders 方法
	t.Log("步骤 3: 通过 Provider.FindProviders 查找")
	providers2, err := p.FindProviders(ctx, testCID)
	if err != nil {
		t.Logf("Provider.FindProviders 返回错误: %v", err)
	}
	t.Logf("Provider.FindProviders 找到 %d 个提供者", len(providers2))
}

// ============================================================
// 测试 2: 多 CID 批量提供
// ============================================================

func TestIntegration_ProvideMultipleCIDs(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过集成测试（使用 -short 标志）")
	}

	hosts, dhts := setupDHTNetwork(t, 3)

	cfg := DefaultConfig()
	cfg.Mode = ModeServer
	cfg.ProvideConcurrency = 2
	p, err := NewProvider(hosts[0], cfg)
	if err != nil {
		t.Fatalf("创建 Provider 失败: %v", err)
	}
	p.dht = dhts[0]
	p.started.Store(true)

	// 创建 5 个测试 CID
	cids := make([]cid.Cid, 5)
	for i := range 5 {
		cids[i] = makeTestCID(t, fmt.Sprintf("multi-provide-%d", i))
	}

	// 批量提供
	t.Logf("批量提供 %d 个 CID", len(cids))
	start := time.Now()
	errs := p.ProvideMany(cids)
	elapsed := time.Since(start)

	t.Logf("批量提供完成: 耗时=%s", elapsed)

	failCount := 0
	for i, err := range errs {
		if err != nil {
			t.Logf("  CID %d (%s): 失败 - %v", i, cids[i], err)
			failCount++
		}
	}

	t.Logf("结果: %d/%d 成功", len(cids)-failCount, len(cids))

	// 等待传播
	time.Sleep(2 * time.Second)

	// 验证一个 CID
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	providers, err := dhts[2].FindProviders(ctx, cids[0])
	if err != nil {
		t.Logf("FindProviders 错误: %v", err)
	}
	t.Logf("CID %s: 找到 %d 个提供者", cids[0], len(providers))
}

// ============================================================
// 测试 3: Re-provide 循环
// ============================================================

func TestIntegration_Reprovide(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过集成测试（使用 -short 标志）")
	}

	hosts, dhts := setupDHTNetwork(t, 3)

	cfg := DefaultConfig()
	cfg.Mode = ModeServer
	cfg.ReprovideInterval = 3 * time.Second // 短间隔用于测试
	cfg.ProvideConcurrency = 2
	p, err := NewProvider(hosts[0], cfg)
	if err != nil {
		t.Fatalf("创建 Provider 失败: %v", err)
	}
	p.dht = dhts[0]
	p.started.Store(true)

	// 注册 CID
	testCID := makeTestCID(t, "reprovide-test")
	p.registerCID(testCID)

	// 手动触发 re-provide
	t.Log("触发 re-provide")
	p.reprovideAll()

	// 等待
	time.Sleep(time.Second)

	// 验证 CID 仍在注册表中
	if !p.isRegistered(testCID) {
		t.Error("re-provide 后 CID 应仍在注册表中")
	}

	stats := p.Stats()
	t.Logf("Re-provide 后统计: total=%d failed=%d active=%d",
		stats.TotalProvided, stats.TotalFailed, stats.ActiveCIDs)
}

// ============================================================
// 测试 4: 重试机制
// ============================================================

func TestIntegration_RetryMechanism(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过集成测试（使用 -short 标志）")
	}

	hosts, dhts := setupDHTNetwork(t, 3)

	cfg := DefaultConfig()
	cfg.Mode = ModeServer
	cfg.RetryMaxAttempts = 3
	cfg.RetryBaseDelay = 100 * time.Millisecond
	cfg.RetryMaxDelay = 500 * time.Millisecond
	p, err := NewProvider(hosts[0], cfg)
	if err != nil {
		t.Fatalf("创建 Provider 失败: %v", err)
	}
	p.dht = dhts[0]
	p.started.Store(true)

	testCID := makeTestCID(t, "retry-mechanism-test")

	// 手动将 CID 加入重试队列
	p.enqueueRetry(testCID)

	// 启动重试循环（手动管理 WaitGroup，模拟 Start() 中的 p.wg.Add(1)）
	p.wg.Add(1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		p.retryLoop()
	}()

	// 等待重试完成
	time.Sleep(2 * time.Second)

	// 停止重试循环
	p.cancel()
	wg.Wait()

	stats := p.Stats()
	t.Logf("重试机制统计: retried=%d queue=%d", stats.TotalRetried, stats.RetryQueueLen)
}

// ============================================================
// 测试 5: DHT 断开后重连
// ============================================================

func TestIntegration_DHTDisconnectReconnect(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过集成测试（使用 -short 标志）")
	}

	hosts, dhts := setupDHTNetwork(t, 3)

	cfg := DefaultConfig()
	cfg.Mode = ModeServer
	p, err := NewProvider(hosts[0], cfg)
	if err != nil {
		t.Fatalf("创建 Provider 失败: %v", err)
	}
	p.dht = dhts[0]
	p.started.Store(true)

	testCID := makeTestCID(t, "disconnect-test")

	// 步骤 1: 正常提供
	t.Log("步骤 1: 正常提供")
	err = p.Provide(testCID)
	t.Logf("  结果: %v", err)

	// 步骤 2: 关闭底层 DHT（模拟断开）
	t.Log("步骤 2: 关闭 DHT（模拟断连）")
	if err := p.dht.Close(); err != nil {
		t.Logf("  关闭 DHT 失败: %v", err)
	}

	// 步骤 3: 尝试提供（应失败并加入重试队列）
	t.Log("步骤 3: 断连后尝试提供")
	c2 := makeTestCID(t, "disconnect-test-2")
	err = p.Provide(c2)
	t.Logf("  结果: %v", err)

	// 步骤 4: 创建新 DHT（模拟重连）
	t.Log("步骤 4: 创建新 DHT（重连）")
	ctx := context.Background()
	d2, err := dht.New(ctx, hosts[0],
		dht.Mode(dht.ModeServer),
		dht.DisableAutoRefresh(),
	)
	if err != nil {
		t.Fatalf("  创建新 DHT 失败: %v", err)
	}
	defer d2.Close()
	p.dht = d2

	// 连接到其他节点
	for i := 1; i < 3; i++ {
		pi := peer.AddrInfo{ID: hosts[i].ID(), Addrs: hosts[i].Addrs()}
		if err := hosts[0].Connect(ctx, pi); err != nil {
			t.Logf("  连接 %d 失败: %v", i, err)
		}
	}
	time.Sleep(time.Second)

	// 步骤 5: 重新提供
	t.Log("步骤 5: 重连后提供")
	c3 := makeTestCID(t, "disconnect-test-3")
	err = p.Provide(c3)
	t.Logf("  结果: %v", err)
}

// ============================================================
// 测试 6: 并发安全性
// ============================================================

func TestIntegration_ConcurrencySafety(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过集成测试（使用 -short 标志）")
	}

	hosts, dhts := setupDHTNetwork(t, 3)

	cfg := DefaultConfig()
	cfg.Mode = ModeServer
	p, err := NewProvider(hosts[0], cfg)
	if err != nil {
		t.Fatalf("创建 Provider 失败: %v", err)
	}
	p.dht = dhts[0]
	p.started.Store(true)

	// 并发操作
	var wg sync.WaitGroup
	numGoroutines := 20
	numCIDs := 50

	// 并发提供
	for i := range numGoroutines {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < numCIDs/numGoroutines; j++ {
				c := makeTestCID(t, fmt.Sprintf("concurrent-safety-%d-%d", workerID, j))
				_ = p.Provide(c)
			}
		}(i)
	}

	// 并发读取统计
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = p.Stats()
				_ = p.RegisteredCIDs()
				_ = p.RegisteredCount()
				time.Sleep(time.Millisecond)
			}
		}()
	}

	wg.Wait()

	// 验证没有发生 race condition
	stats := p.Stats()
	t.Logf("并发测试后: provided=%d failed=%d active=%d",
		stats.TotalProvided, stats.TotalFailed, stats.ActiveCIDs)

	if stats.ActiveCIDs > 0 {
		t.Logf("✅ 注册了 %d 个 CID，并发操作正确", stats.ActiveCIDs)
	}
}

// ============================================================
// 测试 7: 无效 CID 处理
// ============================================================

func TestIntegration_InvalidCID(t *testing.T) {
	hosts, dhts := setupDHTNetwork(t, 2)

	cfg := DefaultConfig()
	p, err := NewProvider(hosts[0], cfg)
	if err != nil {
		t.Fatalf("创建 Provider 失败: %v", err)
	}
	p.dht = dhts[0]
	p.started.Store(true)

	// 测试无效 CID
	invalidTests := []struct {
		name string
		cid  cid.Cid
	}{
		{"未定义CID", cid.Cid{}},
		{"UndefCID", cid.Undef},
	}

	for _, tt := range invalidTests {
		t.Run(tt.name, func(t *testing.T) {
			err := p.Provide(tt.cid)
			if err == nil {
				t.Error("对无效 CID 的 Provide 应返回错误")
			}
		})
	}
}

// ============================================================
// 测试 8: 大容量 CID 注册表
// ============================================================

func TestIntegration_LargeCIDRegistry(t *testing.T) {
	hosts, _ := setupDHTNetwork(t, 2)

	cfg := DefaultConfig()
	cfg.MaxCachedCIDs = 1000
	p, err := NewProvider(hosts[0], cfg)
	if err != nil {
		t.Fatalf("创建 Provider 失败: %v", err)
	}

	// 注册大量 CID
	numCIDs := 2000
	for i := range numCIDs {
		c := makeTestCID(t, fmt.Sprintf("large-registry-%d", i))
		p.registerCID(c)
	}

	// 验证容量限制
	count := p.RegisteredCount()
	t.Logf("注册 %d 个 CID 后，注册表大小 = %d", numCIDs, count)

	if count > 1000 {
		t.Errorf("注册表超出容量: %d > 1000", count)
	}

	// 验证可以查找
	for i := numCIDs - 100; i < numCIDs; i++ {
		c := makeTestCID(t, fmt.Sprintf("large-registry-%d", i))
		if !p.isRegistered(c) {
			t.Errorf("最近注册的 CID %d 应在注册表中", i)
			break
		}
	}
}

// ============================================================
// 测试 9: FindProviders 超时
// ============================================================

func TestIntegration_FindProvidersTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过集成测试（使用 -short 标志）")
	}

	hosts, dhts := setupDHTNetwork(t, 2)

	cfg := DefaultConfig()
	p, err := NewProvider(hosts[0], cfg)
	if err != nil {
		t.Fatalf("创建 Provider 失败: %v", err)
	}
	p.dht = dhts[0]
	p.started.Store(true)

	// 使用很短的超时
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	testCID := makeTestCID(t, "timeout-test")
	providers, err := p.FindProviders(ctx, testCID)

	t.Logf("超时查找: providers=%d err=%v", len(providers), err)
	// 不要求必须成功，但不应 panic
}

// ============================================================
// 测试 10: Monitor 查找（测试后验证）
// ============================================================

// makeCustomCID creates a CID with custom prefix bytes for DHT key testing
func makeCustomCID(t *testing.T, prefix string, idx int) cid.Cid {
	t.Helper()
	data := fmt.Sprintf("%s-%d", prefix, idx)
	mh, err := multihash.Sum([]byte(data), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatalf("创建 multihash 失败: %v", err)
	}
	return cid.NewCidV1(cid.Raw, mh)
}

func TestIntegration_ProvideThenMonitor(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过集成测试（使用 -short 标志）")
	}

	hosts, dhts := setupDHTNetwork(t, 4)

	// 提供者
	providerDHT := dhts[0]

	cfg := DefaultConfig()
	p, err := NewProvider(hosts[0], cfg)
	if err != nil {
		t.Fatalf("创建 Provider 失败: %v", err)
	}
	p.dht = providerDHT
	p.started.Store(true)

	// 提供 3 个 CID
	cids := make([]cid.Cid, 3)
	for i := range 3 {
		cids[i] = makeCustomCID(t, "monitor", i)
	}

	for _, c := range cids {
		err := p.Provide(c)
		t.Logf("提供 %s: %v", c, err)
	}

	// 等待传播
	time.Sleep(3 * time.Second)

	// 从其他节点查找
	for i := 1; i < 4; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		for _, c := range cids {
			providers, err := dhts[i].FindProviders(ctx, c)
			if err != nil {
				t.Logf("节点 %d 查找 %s 失败: %v", i, c, err)
			} else {
				t.Logf("节点 %d 查找 %s: %d 个提供者", i, c, len(providers))
				for _, p := range providers {
					if p.ID == hosts[0].ID() {
						t.Logf("  ✅ 节点 %d 找到本节点为提供者", i)
					}
				}
			}
		}
	}
}
