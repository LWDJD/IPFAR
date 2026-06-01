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
	// AdvertiseInterval 节点广告间隔（定期向 DHT 网络公布连接信息）
	// 0 表示禁用广告循环
	AdvertiseInterval time.Duration
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
		AdvertiseInterval:  30 * time.Minute,
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
	// AdvertiseInterval 不在此设置默认值：0 表示禁用广告循环，
	// 默认值由 DefaultConfig() 提供（30 分钟）。

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

	log.Debug("DHT Provider：创建成功 mode=%s bootstrap_peers=%d", cfg.Mode, len(cfg.BootstrapPeers))

	return p, nil
}

// NewProviderWithExistingHost 使用已有 libp2p host 创建并启动 DHT Provider
//
// 与 NewProviderWithHost 不同，此函数不创建 host，而是接受外部传入的共享 host。
// 适用于 Bitswap 和 DHT 共享同一 host 的桥接架构。
// 内部调用 NewProvider + Start，一步完成创建和启动。
func NewProviderWithExistingHost(host host.Host, cfg Config) (*Provider, error) {
	provider, err := NewProvider(host, cfg)
	if err != nil {
		return nil, err
	}
	if err := provider.Start(); err != nil {
		return nil, fmt.Errorf("启动 Provider 失败: %w", err)
	}
	return provider, nil
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

	// 额外添加纯 IP 的公共引导节点，弥补默认 dnsaddr 节点在 DNS 受限环境下的不足
	additionalBootstrapPeers := []string{
		"/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
		"/ip4/147.75.83.83/tcp/4001/p2p/QmVaU6kR4iQ27FGgoL9KFN2HQPZbMsYcGjpgxrGFBCHjCu",
		"/ip4/147.75.109.213/tcp/4001/p2p/12D3KooWHJtuW81BingU7kwJ2pLkm2DyXBQYBbkDvw2dkzGGSUPi",
	}
	for _, addrStr := range additionalBootstrapPeers {
		addr, err := multiaddr.NewMultiaddr(addrStr)
		if err != nil {
			continue
		}
		peerInfo, err := peer.AddrInfoFromP2pAddr(addr)
		if err != nil {
			continue
		}
		dhtOpts = append(dhtOpts, dht.BootstrapPeers(*peerInfo))
		log.Debug("DHT Provider: 添加额外引导节点 %s", peerInfo.ID)
	}

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

	// 启动 DHT bootstrap（带超时保护，防止网络不通时无限阻塞）
	log.Debug("DHT Provider：开始连接引导节点...")
	bootstrapCtx, bootstrapCancel := context.WithTimeout(p.ctx, 60*time.Second)
	defer bootstrapCancel()
	if err := p.dht.Bootstrap(bootstrapCtx); err != nil {
		log.Warn("DHT Provider: Bootstrap 失败（非致命）: %v", err)
	}
	log.Debug("DHT Provider：引导节点连接完成")

	p.started.Store(true)

	// 启动 re-provide 循环
	p.wg.Add(1)
	go p.reprovideLoop()

	// 启动重试循环
	p.wg.Add(1)
	go p.retryLoop()

	// 启动节点广告循环
	p.startAdvertiseLoop()

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
	log.Debug("DHT Provide：开始发布 CID=%s", c.String())
	err := p.doProvide(c)
	if err != nil {
		log.Debug("DHT Provide：发布失败 CID=%s error=%v", c.String(), err)
		log.Warn("DHT Provider: Provide 失败 %s: %v", c, err)
		p.stats.mu.Lock()
		p.stats.TotalFailed++
		p.stats.LastError = err
		p.stats.mu.Unlock()

		// 加入重试队列
		p.enqueueRetry(c)
		return err
	}

	log.Debug("DHT Provide：发布成功 CID=%s", c.String())

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
	log.Debug("DHT Reprovide：开始重新提供 %d 个 CID", len(cids))

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
	log.Debug("DHT Reprovide：完成")

	p.stats.mu.Lock()
	p.stats.TotalProvided += successCount.Load()
	p.stats.TotalFailed += failCount.Load()
	p.stats.ActiveCIDs = len(p.cidRegistry)
	p.stats.LastProvideTime = time.Now()
	p.stats.mu.Unlock()
}

// startAdvertiseLoop 定期向 DHT 网络公布节点连接信息
//
// 桥节点需要让 DHT 网络中其他节点知道自己的多地址，否则其他节点
// 只知道 peer ID 但连接时报 "no addresses"。
//
// 工作原理：ForceRefresh 触发 doRefresh，其中 queryForSelf 执行
// 对自身 peer ID 的 FIND_NODE 查询。周围节点处理查询时通过 libp2p
// Identify 协议获取我们的地址并更新它们的路由表，达到公布效果。
//
// 启动后立即执行首次公告（延迟 15s 等待 Bootstrap 完成），之后按
// AdvertiseInterval 定期执行。设为 0 可禁用。
func (p *Provider) startAdvertiseLoop() {
	if p.cfg.AdvertiseInterval <= 0 {
		log.Info("DHT Provider: 节点广告循环已禁用（AdvertiseInterval=0）")
		return
	}

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()

		log.Info("DHT Provider: 节点广告循环已启动 interval=%s peer_id=%s",
			p.cfg.AdvertiseInterval, p.host.ID())

		// 首次公告：延迟 15 秒等待 Bootstrap 完成，然后立即公告一次
		select {
		case <-time.After(15 * time.Second):
			p.doAdvertise()
		case <-p.ctx.Done():
			return
		}

		// 后续定期公告
		ticker := time.NewTicker(p.cfg.AdvertiseInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				p.doAdvertise()
			case <-p.ctx.Done():
				return
			}
		}
	}()
}

// doAdvertise 执行一次节点连接信息公告
//
// 通过 ForceRefresh 刷新所有路由桶，同时触发 queryForSelf 让周围节点
// 获取并缓存我们的地址信息。带 30 秒超时保护。
// 超时后回退到 Bootstrap 重连引导节点，确保节点在 DHT 网络中保持可达。
func (p *Provider) doAdvertise() {
	if p.dht == nil {
		log.Warn("DHT Provider: 跳过节点公告，DHT 实例为空")
		return
	}

	addrs := p.host.Addrs()
	pid := p.host.ID()

	// 统计传输类型
	transports := make(map[string]int)
	for _, a := range addrs {
		multiaddr.ForEach(a, func(c multiaddr.Component) bool {
			transports[c.Protocol().Name]++
			return true
		})
	}

	log.Info("DHT Provider: 开始向 DHT 网络公布节点连接信息 peer_id=%s addrs=%d transports=%v",
		pid, len(addrs), transports)

	// ForceRefresh 强制刷新所有路由桶，调用链：
	//   ForceRefresh → doRefresh(true) → queryForSelf + refreshCpl(每个桶)
	//   queryForSelf 执行 FIND_NODE(self)，周边节点通过此过程
	//   更新对我们地址的记录
	errCh := p.dht.ForceRefresh()

	// 等待刷新完成，带超时保护和优雅关闭响应
	select {
	case err := <-errCh:
		if err != nil {
			log.Warn("DHT Provider: 路由表刷新失败 peer_id=%s: %v", pid, err)
			p.stats.mu.Lock()
			p.stats.LastError = fmt.Errorf("节点公告刷新失败: %w", err)
			p.stats.mu.Unlock()

			// 刷新失败时尝试重新 Bootstrap 作为补救
			log.Info("DHT Provider: 路由刷新失败，尝试 Bootstrap 重连...")
			bctx, bcancel := context.WithTimeout(p.ctx, 30*time.Second)
			defer bcancel()
			if berr := p.dht.Bootstrap(bctx); berr != nil {
				log.Warn("DHT Provider: Bootstrap 补救也失败: %v", berr)
			} else {
				log.Info("DHT Provider: Bootstrap 补救成功")
			}
		} else {
			log.Info("DHT Provider: 节点连接信息已成功公布 peer_id=%s addrs=%d transports=%v",
				pid, len(addrs), transports)
		}
	case <-time.After(30 * time.Second):
		log.Warn("DHT Provider: 路由表刷新超时（30s），尝试 Bootstrap 补救 peer_id=%s", pid)
		p.stats.mu.Lock()
		p.stats.LastError = fmt.Errorf("节点公告刷新超时")
		p.stats.mu.Unlock()

		// 超时时也尝试 Bootstrap 作为补救
		bctx, bcancel := context.WithTimeout(p.ctx, 30*time.Second)
		defer bcancel()
		if berr := p.dht.Bootstrap(bctx); berr != nil {
			log.Warn("DHT Provider: Bootstrap 补救也失败: %v", berr)
		} else {
			log.Info("DHT Provider: Bootstrap 补救成功")
		}
	case <-p.ctx.Done():
		log.Debug("DHT Provider: 节点公告被取消（ctx 已关闭）")
		return
	}
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

	log.Debug("DHT 重试：重试 CID=%s 第 %d 次", c, attempt)

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
