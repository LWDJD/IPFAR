// Package dht 提供 IPFS DHT（Kademlia）内容发布功能。
//
// 桥节点启动时作为 DHT server 加入 IPFS 网络，对本地缓存中已有的 CID
// 通过 dht.Provide(ctx, cid) 发布到网络，并支持定期重新提供（re-provide）
// 以防止路由过期。
//
// 规范参考: ipfar-specs/V1/P3-1-DHT内容发布.md
package dht

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/multiformats/go-multiaddr"

	"github.com/lwdjd/IPFAR/internal/log"
)

// Mode 表示 DHT 运行模式
type Mode string

const (
	// ModeServer DHT 服务端模式：加入 DHT 网络，为其他节点提供路由服务
	ModeServer Mode = "server"
	// ModeClient DHT 客户端模式：仅查询和发布，不为其他节点路由
	ModeClient Mode = "client"
)

// Config DHT Provider 配置
type Config struct {
	// Enabled 是否启用 DHT 功能
	Enabled bool
	// Mode DHT 运行模式 (server/client)
	Mode Mode
	// BootstrapPeers 引导节点地址列表
	BootstrapPeers []string
	// ReprovideInterval 重新提供间隔
	ReprovideInterval time.Duration
	// ProvideConcurrency 提供并发数
	ProvideConcurrency int
	// RetryMaxAttempts 重试最大次数（0 表示无限重试）
	RetryMaxAttempts int
	// RetryBaseDelay 重试基础延迟
	RetryBaseDelay time.Duration
	// RetryMaxDelay 重试最大延迟
	RetryMaxDelay time.Duration
	// MaxCachedCIDs 最多缓存的 CID 数量（用于 re-provide）
	MaxCachedCIDs int
}

// DefaultConfig 返回默认配置
func DefaultConfig() Config {
	return Config{
		Enabled:            true,
		Mode:               ModeServer,
		BootstrapPeers:     nil, // 使用 IPFS 默认引导节点
		ReprovideInterval:  12 * time.Hour,
		ProvideConcurrency: 4,
		RetryMaxAttempts:   5,
		RetryBaseDelay:     30 * time.Second,
		RetryMaxDelay:      5 * time.Minute,
		MaxCachedCIDs:      10000,
	}
}

// Provider 管理 DHT 内容发布
//
// 核心职责：
//   - 管理 libp2p DHT 实例
//   - 对 CID 执行 Provide 操作
//   - 定期 re-provide 防止路由过期
//   - 管理失败的 Provide 重试队列
type Provider struct {
	cfg    Config
	host   host.Host
	dht    *dht.IpfsDHT
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// CID 注册表：记录已提供的 CID 及时间戳
	cidRegistry   map[cid.Cid]time.Time
	cidRegistryMu sync.RWMutex

	// 重试队列
	retryQueue   chan cid.Cid
	retryQueueMu sync.Mutex

	// 提供并发控制
	provideSema chan struct{}

	// 统计
	stats ProviderStats

	// 状态
	started  atomic.Bool
	closed   atomic.Bool
}

// ProviderStats 提供器运行统计
type ProviderStats struct {
	mu sync.RWMutex
	// TotalProvided 累计提供次数
	TotalProvided uint64
	// TotalFailed 累计失败次数
	TotalFailed uint64
	// TotalRetried 累计重试次数
	TotalRetried uint64
	// ActiveCIDs 当前注册的 CID 数量
	ActiveCIDs int
	// RetryQueueLen 重试队列长度
	RetryQueueLen int
	// LastProvideTime 最近一次提供时间
	LastProvideTime time.Time
	// LastError 最近一次错误
	LastError error
}

// NewProvider 创建新的 DHT Provider
//
// host 必须是已经初始化的 libp2p host
func NewProvider(host host.Host, cfg Config) (*Provider, error) {
	if host == nil {
		return nil, fmt.Errorf("host is nil")
	}

	if cfg.ProvideConcurrency <= 0 {
		cfg.ProvideConcurrency = 4
	}
	if cfg.RetryBaseDelay <= 0 {
		cfg.RetryBaseDelay = 30 * time.Second
	}
	if cfg.RetryMaxDelay <= 0 {
		cfg.RetryMaxDelay = 5 * time.Minute
	}
	if cfg.MaxCachedCIDs <= 0 {
		cfg.MaxCachedCIDs = 10000
	}

	ctx, cancel := context.WithCancel(context.Background())

	p := &Provider{
		cfg:          cfg,
		host:         host,
		ctx:          ctx,
		cancel:       cancel,
		cidRegistry:  make(map[cid.Cid]time.Time),
		retryQueue:   make(chan cid.Cid, 1024),
		provideSema:  make(chan struct{}, cfg.ProvideConcurrency),
	}

	return p, nil
}

// Start 启动 DHT Provider
//
// 初始化 DHT 实例，连接到引导节点，启动 re-provide 循环和重试循环。
func (p *Provider) Start() error {
	if p.closed.Load() {
		return fmt.Errorf("provider is closed")
	}
	if p.started.Load() {
		return fmt.Errorf("provider already started")
	}

	log.Info("DHT Provider: 启动中 mode=%s concurrency=%d reprovide_interval=%s",
		p.cfg.Mode, p.cfg.ProvideConcurrency, p.cfg.ReprovideInterval)

	// 构建 DHT 选项
	var dhtOpts []dht.Option

	switch p.cfg.Mode {
	case ModeServer:
		dhtOpts = append(dhtOpts, dht.Mode(dht.ModeServer))
	case ModeClient:
		dhtOpts = append(dhtOpts, dht.Mode(dht.ModeClient))
	default:
		dhtOpts = append(dhtOpts, dht.Mode(dht.ModeServer))
	}

	// 始终添加官方默认引导节点
	dhtOpts = append(dhtOpts, dht.BootstrapPeers(dht.GetDefaultBootstrapPeerAddrInfos()...))

	// 额外添加用户自定义引导节点
	if len(p.cfg.BootstrapPeers) > 0 {
		for _, addrStr := range p.cfg.BootstrapPeers {
			addr, err := multiaddr.NewMultiaddr(addrStr)
			if err != nil {
				log.Warn("DHT Provider: 无效的引导节点地址 %q: %v", addrStr, err)
				continue
			}
			peerInfo, err := peer.AddrInfoFromP2pAddr(addr)
			if err != nil {
				log.Warn("DHT Provider: 无法解析引导节点 %q: %v", addrStr, err)
				continue
			}
			dhtOpts = append(dhtOpts, dht.BootstrapPeers(*peerInfo))
		}
	}

	// 创建 DHT 实例
	d, err := dht.New(p.ctx, p.host, dhtOpts...)
	if err != nil {
		return fmt.Errorf("创建 DHT 实例失败: %w", err)
	}
	p.dht = d

	// 启动 DHT bootstrap
	if err := p.dht.Bootstrap(p.ctx); err != nil {
		log.Warn("DHT Provider: Bootstrap 失败（非致命）: %v", err)
	}

	p.started.Store(true)

	// 启动 re-provide 循环
	p.wg.Add(1)
	go p.reprovideLoop()

	// 启动重试循环
	p.wg.Add(1)
	go p.retryLoop()

	log.Info("DHT Provider: 已启动 peer_id=%s", p.host.ID())
	return nil
}

// Stop 停止 DHT Provider
func (p *Provider) Stop() error {
	if !p.started.Load() {
		return nil
	}
	if !p.closed.CompareAndSwap(false, true) {
		return nil // 已关闭
	}

	log.Info("DHT Provider: 正在停止...")
	p.cancel()

	if p.dht != nil {
		if err := p.dht.Close(); err != nil {
			log.Warn("DHT Provider: 关闭 DHT 实例失败: %v", err)
		}
	}

	p.wg.Wait()
	log.Info("DHT Provider: 已停止")
	return nil
}

// Provide 发布单个 CID 到 DHT 网络
//
// 将 CID 注册到内部注册表，并通过 DHT 发布 provider 记录。
// 如果提供失败，CID 会自动加入重试队列。
// 返回 nil 表示成功发布（或已注册，跳过重复发布）。
func (p *Provider) Provide(c cid.Cid) error {
	if !p.started.Load() {
		return fmt.Errorf("provider not started")
	}
	if p.closed.Load() {
		return fmt.Errorf("provider is closed")
	}

	// 验证 CID
	if !c.Defined() {
		return fmt.Errorf("invalid CID: undefined")
	}

	// 检查是否已注册
	if p.isRegistered(c) {
		log.Debug("DHT Provider: CID 已注册，跳过 %s", c)
		return nil
	}

	// 获取并发信号量
	select {
	case p.provideSema <- struct{}{}:
		defer func() { <-p.provideSema }()
	case <-p.ctx.Done():
		return p.ctx.Err()
	}

	// 执行提供
	err := p.doProvide(c)
	if err != nil {
		log.Warn("DHT Provider: Provide 失败 %s: %v", c, err)
		p.stats.mu.Lock()
		p.stats.TotalFailed++
		p.stats.LastError = err
		p.stats.mu.Unlock()

		// 加入重试队列
		p.enqueueRetry(c)
		return err
	}

	// 注册成功
	p.registerCID(c)

	p.stats.mu.Lock()
	p.stats.TotalProvided++
	p.stats.LastProvideTime = time.Now()
	p.stats.ActiveCIDs = len(p.cidRegistry)
	p.stats.mu.Unlock()

	log.Info("DHT Provider: Provide 成功 %s", c)
	return nil
}

// ProvideMany 批量发布多个 CID
//
// 并发度受 ProvideConcurrency 配置控制。
func (p *Provider) ProvideMany(cids []cid.Cid) []error {
	errors := make([]error, len(cids))
	var wg sync.WaitGroup

	for i, c := range cids {
		wg.Add(1)
		go func(idx int, cid cid.Cid) {
			defer wg.Done()
			errors[idx] = p.Provide(cid)
		}(i, c)
	}

	wg.Wait()
	return errors
}

// doProvide 执行单次 DHT Provide 操作
func (p *Provider) doProvide(c cid.Cid) error {
	if p.dht == nil {
		return fmt.Errorf("DHT instance is nil")
	}

	ctx, cancel := context.WithTimeout(p.ctx, 30*time.Second)
	defer cancel()

	return p.dht.Provide(ctx, c, true)
}

// registerCID 将 CID 注册到内部注册表
func (p *Provider) registerCID(c cid.Cid) {
	p.cidRegistryMu.Lock()
	defer p.cidRegistryMu.Unlock()

	// 容量控制：如果超过最大数量，移除最旧的条目
	if len(p.cidRegistry) >= p.cfg.MaxCachedCIDs {
		var oldest cid.Cid
		var oldestTime time.Time
		first := true
		for k, v := range p.cidRegistry {
			if first || v.Before(oldestTime) {
				oldest = k
				oldestTime = v
				first = false
			}
		}
		delete(p.cidRegistry, oldest)
	}

	p.cidRegistry[c] = time.Now()
}

// isRegistered 检查 CID 是否已注册
func (p *Provider) isRegistered(c cid.Cid) bool {
	p.cidRegistryMu.RLock()
	defer p.cidRegistryMu.RUnlock()
	_, ok := p.cidRegistry[c]
	return ok
}

// unregisterCID 从注册表中移除 CID
func (p *Provider) unregisterCID(c cid.Cid) {
	p.cidRegistryMu.Lock()
	defer p.cidRegistryMu.Unlock()
	delete(p.cidRegistry, c)
}

// reprovideLoop 定期重新提供循环
//
// 按 ReprovideInterval 间隔遍历所有已注册 CID 并重新提供。
func (p *Provider) reprovideLoop() {
	defer p.wg.Done()

	if p.cfg.ReprovideInterval <= 0 {
		log.Info("DHT Provider: re-provide 已禁用（间隔 <= 0）")
		return
	}

	ticker := time.NewTicker(p.cfg.ReprovideInterval)
	defer ticker.Stop()

	log.Info("DHT Provider: re-provide 循环已启动 interval=%s", p.cfg.ReprovideInterval)

	for {
		select {
		case <-ticker.C:
			p.reprovideAll()
		case <-p.ctx.Done():
			return
		}
	}
}

// reprovideAll 重新提供所有已注册的 CID
func (p *Provider) reprovideAll() {
	p.cidRegistryMu.RLock()
	cids := make([]cid.Cid, 0, len(p.cidRegistry))
	for c := range p.cidRegistry {
		cids = append(cids, c)
	}
	p.cidRegistryMu.RUnlock()

	if len(cids) == 0 {
		log.Debug("DHT Provider: 无 CID 需要 re-provide")
		return
	}

	log.Info("DHT Provider: 开始 re-provide count=%d", len(cids))

	// 使用信号量控制并发
	sem := make(chan struct{}, p.cfg.ProvideConcurrency)
	var wg sync.WaitGroup
	var successCount, failCount atomic.Uint64

	for _, c := range cids {
		select {
		case sem <- struct{}{}:
		case <-p.ctx.Done():
			return
		}

		wg.Add(1)
		go func(cid cid.Cid) {
			defer wg.Done()
			defer func() { <-sem }()

			if err := p.doProvide(cid); err != nil {
				log.Warn("DHT Provider: re-provide 失败 %s: %v", cid, err)
				failCount.Add(1)
				p.enqueueRetry(cid)
			} else {
				successCount.Add(1)
			}

			// 更新时间戳
			p.cidRegistryMu.Lock()
			if _, ok := p.cidRegistry[cid]; ok {
				p.cidRegistry[cid] = time.Now()
			}
			p.cidRegistryMu.Unlock()
		}(c)
	}

	wg.Wait()

	log.Info("DHT Provider: re-provide 完成 success=%d failed=%d total=%d",
		successCount.Load(), failCount.Load(), len(cids))

	p.stats.mu.Lock()
	p.stats.TotalProvided += successCount.Load()
	p.stats.TotalFailed += failCount.Load()
	p.stats.ActiveCIDs = len(p.cidRegistry)
	p.stats.LastProvideTime = time.Now()
	p.stats.mu.Unlock()
}

// retryLoop 重试循环
//
// 处理 retryQueue 中的失败 CID，使用指数退避策略。
func (p *Provider) retryLoop() {
	defer p.wg.Done()

	log.Info("DHT Provider: 重试循环已启动")

	for {
		select {
		case c := <-p.retryQueue:
			p.retryProvide(c, 0)
		case <-p.ctx.Done():
			return
		}
	}
}

// retryProvide 带退避的 Provide 重试
func (p *Provider) retryProvide(c cid.Cid, attempt int) {
	if p.cfg.RetryMaxAttempts > 0 && attempt >= p.cfg.RetryMaxAttempts {
		log.Warn("DHT Provider: 已达最大重试次数 max=%d cid=%s", p.cfg.RetryMaxAttempts, c)
		return
	}

	// 计算退避延迟
	delay := p.cfg.RetryBaseDelay * time.Duration(1<<uint(attempt))
	if delay > p.cfg.RetryMaxDelay {
		delay = p.cfg.RetryMaxDelay
	}

	log.Debug("DHT Provider: 等待 %s 后重试 (attempt=%d) cid=%s", delay, attempt, c)

	select {
	case <-time.After(delay):
	case <-p.ctx.Done():
		return
	}

	// 检查是否已关闭
	if p.closed.Load() {
		return
	}

	// 获取并发信号量
	select {
	case p.provideSema <- struct{}{}:
		defer func() { <-p.provideSema }()
	case <-p.ctx.Done():
		return
	}

	err := p.doProvide(c)
	if err != nil {
		log.Warn("DHT Provider: 重试失败 (attempt=%d) cid=%s: %v", attempt, c, err)
		p.stats.mu.Lock()
		p.stats.TotalRetried++
		p.stats.LastError = err
		p.stats.mu.Unlock()

		// 继续重试
		go p.retryProvide(c, attempt+1)
		return
	}

	p.registerCID(c)
	log.Info("DHT Provider: 重试成功 (attempt=%d) cid=%s", attempt, c)

	p.stats.mu.Lock()
	p.stats.TotalProvided++
	p.stats.TotalRetried++
	p.stats.ActiveCIDs = len(p.cidRegistry)
	p.stats.LastProvideTime = time.Now()
	p.stats.mu.Unlock()
}

// enqueueRetry 将 CID 加入重试队列（非阻塞）
func (p *Provider) enqueueRetry(c cid.Cid) {
	select {
	case p.retryQueue <- c:
		p.stats.mu.Lock()
		p.stats.RetryQueueLen = len(p.retryQueue)
		p.stats.mu.Unlock()
	default:
		log.Warn("DHT Provider: 重试队列已满，丢弃 cid=%s", c)
	}
}

// Stats 获取统计信息
func (p *Provider) Stats() ProviderStats {
	p.stats.mu.RLock()
	defer p.stats.mu.RUnlock()

	stats := p.stats
	stats.ActiveCIDs = len(p.cidRegistry)
	stats.RetryQueueLen = len(p.retryQueue)
	return stats
}

// IsStarted 检查是否已启动
func (p *Provider) IsStarted() bool {
	return p.started.Load()
}

// IsClosed 检查是否已关闭
func (p *Provider) IsClosed() bool {
	return p.closed.Load()
}

// RegisteredCIDs 返回所有已注册的 CID
func (p *Provider) RegisteredCIDs() []cid.Cid {
	p.cidRegistryMu.RLock()
	defer p.cidRegistryMu.RUnlock()

	cids := make([]cid.Cid, 0, len(p.cidRegistry))
	for c := range p.cidRegistry {
		cids = append(cids, c)
	}
	return cids
}

// RegisteredCount 返回已注册 CID 数量
func (p *Provider) RegisteredCount() int {
	p.cidRegistryMu.RLock()
	defer p.cidRegistryMu.RUnlock()
	return len(p.cidRegistry)
}

// FindProviders 查找提供指定 CID 的节点
//
// 用于测试验证 Provide 是否成功。
func (p *Provider) FindProviders(ctx context.Context, c cid.Cid) ([]peer.AddrInfo, error) {
	if p.dht == nil {
		return nil, fmt.Errorf("DHT instance is nil")
	}

	resultCh := p.dht.FindProvidersAsync(ctx, c, 20)
	var providers []peer.AddrInfo
	for p := range resultCh {
		providers = append(providers, p)
	}

	return providers, nil
}

// Host 返回底层 libp2p host
func (p *Provider) Host() host.Host {
	return p.host
}

// DHT 返回底层 DHT 实例
func (p *Provider) DHT() *dht.IpfsDHT {
	return p.dht
}
