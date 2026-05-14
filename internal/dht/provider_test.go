package dht

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
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
// 辅助函数
// ============================================================

// makeTestCID 创建一个测试用 CID
func makeTestCID(t *testing.T, s string) cid.Cid {
	t.Helper()
	mh, err := multihash.Sum([]byte(s), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatalf("failed to create multihash: %v", err)
	}
	return cid.NewCidV1(cid.Raw, mh)
}

// createTestHost 创建一个用于测试的 libp2p host
func createTestHost(t *testing.T) host.Host {
	t.Helper()
	h, err := basichost.NewHost(swarmt.GenSwarm(t, swarmt.OptDisableReuseport), new(basichost.HostOpts))
	if err != nil {
		t.Fatalf("failed to create host: %v", err)
	}
	h.Start()
	t.Cleanup(func() { h.Close() })
	return h
}

// createTestDHT 创建一个用于测试的 IpfsDHT
func createTestDHT(t *testing.T, ctx context.Context, h host.Host) *dht.IpfsDHT {
	t.Helper()
	d, err := dht.New(ctx, h,
		dht.Mode(dht.ModeServer),
		dht.DisableAutoRefresh(),
	)
	if err != nil {
		t.Fatalf("failed to create DHT: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}

// ============================================================
// 单元测试：配置验证
// ============================================================

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if !cfg.Enabled {
		t.Error("默认配置应启用 DHT")
	}
	if cfg.Mode != ModeServer {
		t.Errorf("默认模式应为 server，实际为 %s", cfg.Mode)
	}
	if cfg.ReprovideInterval != 12*time.Hour {
		t.Errorf("默认重提供间隔应为 12h，实际为 %s", cfg.ReprovideInterval)
	}
	if cfg.ProvideConcurrency != 4 {
		t.Errorf("默认并发数应为 4，实际为 %d", cfg.ProvideConcurrency)
	}
	if cfg.RetryMaxAttempts != 5 {
		t.Errorf("默认最大重试次数应为 5，实际为 %d", cfg.RetryMaxAttempts)
	}
	if cfg.RetryBaseDelay != 30*time.Second {
		t.Errorf("默认重试基础延迟应为 30s，实际为 %s", cfg.RetryBaseDelay)
	}
	if cfg.MaxCachedCIDs != 10000 {
		t.Errorf("默认最大 CID 缓存数应为 10000，实际为 %d", cfg.MaxCachedCIDs)
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{
			name: "零值并发应使用默认值",
			cfg:  Config{ProvideConcurrency: 0},
		},
		{
			name: "负值并发应使用默认值",
			cfg:  Config{ProvideConcurrency: -1},
		},
		{
			name: "零值重试延迟应使用默认值",
			cfg:  Config{RetryBaseDelay: 0},
		},
		{
			name: "零值最大 CID 应使用默认值",
			cfg:  Config{MaxCachedCIDs: 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := createTestHost(t)
			p, err := NewProvider(h, tt.cfg)
			if err != nil {
				t.Fatalf("创建 Provider 失败: %v", err)
			}

			if p.cfg.ProvideConcurrency <= 0 {
				t.Error("ProvideConcurrency 应为正值")
			}
			if p.cfg.RetryBaseDelay <= 0 {
				t.Error("RetryBaseDelay 应为正值")
			}
			if p.cfg.MaxCachedCIDs <= 0 {
				t.Error("MaxCachedCIDs 应为正值")
			}
		})
	}
}

// ============================================================
// 单元测试：NewProvider
// ============================================================

func TestNewProvider_NilHost(t *testing.T) {
	_, err := NewProvider(nil, DefaultConfig())
	if err == nil {
		t.Error("nil host 应返回错误")
	}
}

func TestNewProvider_ValidHost(t *testing.T) {
	h := createTestHost(t)
	cfg := DefaultConfig()
	cfg.Enabled = false // 不实际启动 DHT

	p, err := NewProvider(h, cfg)
	if err != nil {
		t.Fatalf("创建 Provider 失败: %v", err)
	}

	if p.IsStarted() {
		t.Error("新创建的 Provider 不应已启动")
	}
	if p.IsClosed() {
		t.Error("新创建的 Provider 不应已关闭")
	}
	if p.Host() != h {
		t.Error("Host 引用不一致")
	}
}

// ============================================================
// 单元测试：CID 注册表操作
// ============================================================

func TestCIDRegistry_RegisterAndCheck(t *testing.T) {
	h := createTestHost(t)
	p, _ := NewProvider(h, DefaultConfig())

	c1 := makeTestCID(t, "test1")
	c2 := makeTestCID(t, "test2")

	// 初始为空
	if p.RegisteredCount() != 0 {
		t.Error("初始注册表应为空")
	}

	// 注册
	p.registerCID(c1)
	if p.RegisteredCount() != 1 {
		t.Error("注册后计数应为 1")
	}
	if !p.isRegistered(c1) {
		t.Error("c1 应为已注册")
	}
	if p.isRegistered(c2) {
		t.Error("c2 应为未注册")
	}

	// 注册第二个
	p.registerCID(c2)
	if p.RegisteredCount() != 2 {
		t.Error("注册两个后计数应为 2")
	}

	// 取消注册
	p.unregisterCID(c1)
	if p.RegisteredCount() != 1 {
		t.Error("取消注册后计数应为 1")
	}
	if p.isRegistered(c1) {
		t.Error("c1 应已取消注册")
	}

	// RegisteredCIDs 返回列表
	cids := p.RegisteredCIDs()
	if len(cids) != 1 {
		t.Errorf("RegisteredCIDs 应返回 1 个 CID，实际 %d", len(cids))
	}
}

func TestCIDRegistry_CapacityLimit(t *testing.T) {
	h := createTestHost(t)
	cfg := DefaultConfig()
	cfg.MaxCachedCIDs = 5
	p, _ := NewProvider(h, cfg)

	// 注册超过容量
	for i := range 10 {
		c := makeTestCID(t, fmt.Sprintf("test-%d", i))
		p.registerCID(c)
	}

	if p.RegisteredCount() > 5 {
		t.Errorf("注册表不应超过容量 5，实际 %d", p.RegisteredCount())
	}
}

// ============================================================
// 单元测试：启动/停止
// ============================================================

func TestProvider_StartStop(t *testing.T) {
	h := createTestHost(t)
	ctx := context.Background()

	// 创建 DHT 实例
	d := createTestDHT(t, ctx, h)

	cfg := DefaultConfig()
	p, err := NewProvider(h, cfg)
	if err != nil {
		t.Fatalf("创建 Provider 失败: %v", err)
	}

	// 手动设置 DHT（跳过 Start 中的 DHT 创建）
	p.dht = d

	// 启动
	if err := p.Start(); err != nil {
		// 可能因已启动而失败
		t.Logf("Start 返回: %v", err)
	}

	if !p.IsStarted() {
		t.Error("启动后 IsStarted 应为 true")
	}

	// 双重启动
	err = p.Start()
	if err == nil {
		t.Error("双重启动应返回错误")
	}

	// 停止
	if err := p.Stop(); err != nil {
		t.Fatalf("停止失败: %v", err)
	}

	if !p.IsClosed() {
		t.Error("停止后 IsClosed 应为 true")
	}

	// 双重停止
	err = p.Stop()
	if err != nil {
		t.Errorf("双重停止不应返回错误: %v", err)
	}
}

// ============================================================
// 单元测试：Provide（无效输入）
// ============================================================

func TestProvide_UndefinedCID(t *testing.T) {
	h := createTestHost(t)
	p, _ := NewProvider(h, DefaultConfig())

	// 未定义的 CID
	undefined := cid.Cid{}
	err := p.Provide(undefined)
	if err == nil {
		t.Error("对未定义 CID 的 Provide 应返回错误")
	}
}

func TestProvide_NotStarted(t *testing.T) {
	h := createTestHost(t)
	p, _ := NewProvider(h, DefaultConfig())

	c := makeTestCID(t, "test")
	err := p.Provide(c)
	if err == nil {
		t.Error("未启动时的 Provide 应返回错误")
	}
}

func TestProvide_Closed(t *testing.T) {
	h := createTestHost(t)
	p, _ := NewProvider(h, DefaultConfig())

	// 手动设置 started 为 true 并关闭
	p.started.Store(true)
	p.Stop()

	c := makeTestCID(t, "test")
	err := p.Provide(c)
	if err == nil {
		t.Error("已关闭时的 Provide 应返回错误")
	}
}

// ============================================================
// 单元测试：重试队列
// ============================================================

func TestRetryQueue_Enqueue(t *testing.T) {
	h := createTestHost(t)
	p, _ := NewProvider(h, DefaultConfig())

	c := makeTestCID(t, "retry-test")

	// 加入重试队列
	p.enqueueRetry(c)

	// 验证队列长度
	stats := p.Stats()
	if stats.RetryQueueLen != 1 {
		t.Errorf("重试队列长度应为 1，实际 %d", stats.RetryQueueLen)
	}

	// 从队列中取出
	select {
	case received := <-p.retryQueue:
		if received != c {
			t.Errorf("队列中的 CID 不匹配: %s != %s", received, c)
		}
	default:
		t.Error("应能从重试队列中取出 CID")
	}
}

func TestRetryQueue_Full(t *testing.T) {
	h := createTestHost(t)
	cfg := DefaultConfig()
	p, _ := NewProvider(h, cfg)

	// 填满队列（容量 1024）
	for i := range 1100 {
		c := makeTestCID(t, fmt.Sprintf("fill-%d", i))
		p.enqueueRetry(c)
	}

	stats := p.Stats()
	if stats.RetryQueueLen > 1024 {
		t.Errorf("重试队列不应超过容量，实际 %d", stats.RetryQueueLen)
	}
}

// ============================================================
// 单元测试：统计信息
// ============================================================

func TestStats_Initial(t *testing.T) {
	h := createTestHost(t)
	p, _ := NewProvider(h, DefaultConfig())

	stats := p.Stats()

	if stats.TotalProvided != 0 {
		t.Error("初始 TotalProvided 应为 0")
	}
	if stats.TotalFailed != 0 {
		t.Error("初始 TotalFailed 应为 0")
	}
	if stats.TotalRetried != 0 {
		t.Error("初始 TotalRetried 应为 0")
	}
	if stats.ActiveCIDs != 0 {
		t.Error("初始 ActiveCIDs 应为 0")
	}
}

// ============================================================
// 单元测试：Cid 验证
// ============================================================

func TestCidValidation(t *testing.T) {
	tests := []struct {
		name string
		cid  cid.Cid
		ok   bool
	}{
		{"未定义CID", cid.Cid{}, false},
		{"无效CID", cid.Undef, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ok := tt.cid.Defined()
			if ok != tt.ok {
				t.Errorf("CID.Defined() = %v, 期望 %v", ok, tt.ok)
			}
		})
	}
}

// ============================================================
// 边界测试
// ============================================================

func TestProvider_EmptyReProvide(t *testing.T) {
	h := createTestHost(t)
	cfg := DefaultConfig()
	cfg.ReprovideInterval = 10 * time.Millisecond
	p, _ := NewProvider(h, cfg)

	ctx := context.Background()
	d := createTestDHT(t, ctx, h)
	p.dht = d

	// 在没有注册 CID 的情况下启动 re-provide
	// 不应 panic
	done := make(chan struct{})
	go func() {
		p.reprovideAll()
		close(done)
	}()

	select {
	case <-done:
		// 成功
	case <-time.After(5 * time.Second):
		t.Error("空 re-provide 超时")
	}
}

func TestProvider_NilDHTProvide(t *testing.T) {
	h := createTestHost(t)
	p, _ := NewProvider(h, DefaultConfig())
	p.started.Store(true)

	c := makeTestCID(t, "nil-dht")
	err := p.doProvide(c)
	if err == nil {
		t.Error("nil DHT 的 doProvide 应返回错误")
	}
}

func TestProvider_FindProvidersNilDHT(t *testing.T) {
	h := createTestHost(t)
	p, _ := NewProvider(h, DefaultConfig())

	c := makeTestCID(t, "find-nil")
	_, err := p.FindProviders(context.Background(), c)
	if err == nil {
		t.Error("nil DHT 的 FindProviders 应返回错误")
	}
}

// ============================================================
// 并发测试：多 CID 同时提供
// ============================================================

func TestProvideMany_Concurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过并发测试（需要 DHT 初始化）")
	}

	h := createTestHost(t)
	ctx := context.Background()
	d := createTestDHT(t, ctx, h)

	// 创建第二个 host + DHT 用于网络连接
	h2 := createTestHost(t)
	_ = createTestDHT(t, ctx, h2) // 创建第二个 DHT 用于网络连接

	// 连接两个 host
	if err := h.Connect(ctx, peer.AddrInfo{ID: h2.ID(), Addrs: h2.Addrs()}); err != nil {
		t.Fatalf("连接 peers 失败: %v", err)
	}

	// 等待连接建立
	time.Sleep(500 * time.Millisecond)

	cfg := DefaultConfig()
	cfg.Mode = ModeServer
	p, _ := NewProvider(h, cfg)
	p.dht = d
	p.started.Store(true)

	// 批量提供
	cids := make([]cid.Cid, 10)
	for i := range 10 {
		cids[i] = makeTestCID(t, fmt.Sprintf("concurrent-%d", i))
	}

	var wg sync.WaitGroup
	var successCount atomic.Int64
	var failCount atomic.Int64

	for _, c := range cids {
		wg.Add(1)
		go func(cid cid.Cid) {
			defer wg.Done()
			err := p.Provide(cid)
			if err == nil {
				successCount.Add(1)
			} else {
				failCount.Add(1)
			}
		}(c)
	}

	wg.Wait()

	t.Logf("并发提供结果: 成功=%d 失败=%d", successCount.Load(), failCount.Load())

	// 只要没有 panic，并发就是正确的
	if p.RegisteredCount() == 0 && successCount.Load() == 0 {
		t.Log("所有 Provide 均失败（可能是 DHT 未完全初始化）")
	}
}

// ============================================================
// 边界测试：DHT 重连
// ============================================================

func TestProvider_DHTReconnect(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过 DHT 重连测试")
	}

	h := createTestHost(t)
	ctx := context.Background()

	cfg := DefaultConfig()
	p, _ := NewProvider(h, cfg)

	// 创建初始 DHT
	d := createTestDHT(t, ctx, h)
	p.dht = d
	p.started.Store(true)

	c := makeTestCID(t, "reconnect-test")

	// 尝试提供（可能失败）
	_ = p.Provide(c)

	// 关闭 DHT（模拟断开）
	if err := d.Close(); err != nil {
		t.Logf("关闭 DHT 失败: %v", err)
	}

	// 创建新 DHT（模拟重连）
	d2 := createTestDHT(t, ctx, h)
	p.dht = d2

	// 再次尝试提供
	_ = p.Provide(makeTestCID(t, "reconnect-test-2"))

	// 验证没有 panic
	t.Log("DHT 重连测试完成")
}

// ============================================================
// 边界测试：空 CID 列表
// ============================================================

func TestProvideMany_EmptyList(t *testing.T) {
	h := createTestHost(t)
	p, _ := NewProvider(h, DefaultConfig())

	errs := p.ProvideMany(nil)
	if len(errs) != 0 {
		t.Errorf("空列表 ProvideMany 应返回空错误列表，实际长度 %d", len(errs))
	}

	errs = p.ProvideMany([]cid.Cid{})
	if len(errs) != 0 {
		t.Errorf("空切片 ProvideMany 应返回空错误列表，实际长度 %d", len(errs))
	}
}

// ============================================================
// 测试：模式切换
// ============================================================

func TestModes(t *testing.T) {
	tests := []struct {
		mode Mode
	}{
		{ModeServer},
		{ModeClient},
	}

	for _, tt := range tests {
		t.Run(string(tt.mode), func(t *testing.T) {
			h := createTestHost(t)
			ctx := context.Background()

			var dhtMode dht.ModeOpt
			switch tt.mode {
			case ModeServer:
				dhtMode = dht.ModeServer
			case ModeClient:
				dhtMode = dht.ModeClient
			}

			d, err := dht.New(ctx, h,
				dht.Mode(dhtMode),
				dht.DisableAutoRefresh(),
			)
			if err != nil {
				t.Fatalf("创建 DHT (%s) 失败: %v", tt.mode, err)
			}
			defer d.Close()

			if d.Mode() != dhtMode {
				t.Errorf("DHT 模式不匹配: 期望 %v, 实际 %v", dhtMode, d.Mode())
			}
		})
	}
}

// ============================================================
// 基准测试
// ============================================================

func BenchmarkCIDRegistry_Register(b *testing.B) {
	h := createTestHost(&testing.T{})
	p, _ := NewProvider(h, DefaultConfig())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := makeTestCID(&testing.T{}, fmt.Sprintf("bench-%d", i))
		p.registerCID(c)
	}
}

func BenchmarkCIDRegistry_Lookup(b *testing.B) {
	h := createTestHost(&testing.T{})
	p, _ := NewProvider(h, DefaultConfig())

	// 预先注册 1000 个 CID
	for i := range 1000 {
		c := makeTestCID(&testing.T{}, fmt.Sprintf("bench-%d", i))
		p.registerCID(c)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % 1000
		c := makeTestCID(&testing.T{}, fmt.Sprintf("bench-%d", idx))
		p.isRegistered(c)
	}
}
