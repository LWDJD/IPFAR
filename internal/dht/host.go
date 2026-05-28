// Package dht 提供 IPFS DHT（Kademlia）内容发布功能。
//
// 本文件实现 libp2p host 的完整创建，包括：
//   - 密钥对生成与身份
//   - 监听地址配置（TCP / QUIC-v1 / WebTransport / WebRTC Direct / IPv4+IPv6）
//   - NAT 穿透（UPnP / NAT-PMP / Hole Punching / AutoNAT v2 / AutoRelay）
//   - 连接管理器与资源管理器
//   - mDNS 局域网发现
//
// 规范参考: ipfar-specs/V1/P3-1-DHT内容发布.md

package dht

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	tls "github.com/libp2p/go-libp2p/p2p/security/tls"
	quic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	libp2pwebrtc "github.com/libp2p/go-libp2p/p2p/transport/webrtc"
	"github.com/libp2p/go-libp2p/p2p/transport/websocket"
	webtransport "github.com/libp2p/go-libp2p/p2p/transport/webtransport"
	"github.com/multiformats/go-multiaddr"

	"github.com/lwdjd/IPFAR/internal/log"
	"github.com/lwdjd/IPFAR/internal/version"
)

// HostConfig libp2p host 配置
type HostConfig struct {
	// ListenAddresses 监听地址列表，如 "/ip4/0.0.0.0/tcp/4001"
	// 为空时使用默认值
	ListenAddresses []string

	// PrivateKey 可选：提供已有私钥（PEM 格式字节）
	// 为空时自动生成 Ed25519 密钥对
	PrivateKey string

	// EnableRelay 是否启用中继（默认 true）
	EnableRelay bool

	// EnableAutoNAT 是否启用 AutoNAT v1（默认 true）
	// 若同时启用 EnableAutoNATv2，v2 优先级更高
	EnableAutoNAT bool

	// EnableNATPortMap 是否启用 UPnP/NAT-PMP 端口映射（默认 true）
	EnableNATPortMap bool

	// EnableHolePunching 是否启用 NAT 打洞（默认 true）
	// 让 NAT 后的节点能建立直接连接，无需中继
	EnableHolePunching bool

	// EnableAutoRelay 是否启用自动中继发现（默认 true）
	// 自动发现中继服务器，在私有网络中通告中继地址
	EnableAutoRelay bool

	// EnableAutoNATv2 是否启用 AutoNAT v2（默认 true）
	// 更高效的 NAT 类型检测协议，替换旧版 AutoNAT
	EnableAutoNATv2 bool

	// ConnMgrLow 连接数下限（默认 100）
	ConnMgrLow int
	// ConnMgrHigh 连接数上限（默认 400）
	ConnMgrHigh int
	// ConnMgrGrace 优雅关闭时间（默认 20s）
	ConnMgrGrace time.Duration

	// ResourceManager 资源管理器，nil 则使用默认限制
	ResourceManager network.ResourceManager

	// BootstrapPeers 引导节点地址列表（multiaddr 字符串）
	BootstrapPeers []string

	// KeyPath 节点私钥持久化路径（PEM 格式 Ed25519 私钥）
	// 为空时生成临时密钥（不持久化），默认 "node.key"
	KeyPath string `json:"key_path"`
}

// DefaultHostConfig 返回默认 host 配置
func DefaultHostConfig() HostConfig {
	return HostConfig{
		ListenAddresses: []string{
			"/ip4/0.0.0.0/tcp/4001",
			"/ip4/0.0.0.0/udp/4001/quic-v1",
			"/ip4/0.0.0.0/udp/4001/quic-v1/webtransport",
			"/ip6/::/tcp/4001",
			"/ip6/::/udp/4001/quic-v1",
			"/ip6/::/udp/4001/quic-v1/webtransport",
			"/ip4/0.0.0.0/udp/4001/webrtc-direct",
			"/ip6/::/udp/4001/webrtc-direct",
		},
		EnableRelay:        true,
		EnableAutoNAT:      true,
		EnableNATPortMap:   true,
		EnableHolePunching: true,
		EnableAutoRelay:    true,
		EnableAutoNATv2:    true,
		ConnMgrLow:         100,
		ConnMgrHigh:        400,
		ConnMgrGrace:       20 * time.Second,
		KeyPath:            "node.key",
	}
}

// NewHost 创建并启动一个完整的 libp2p host
//
// 支持的传输层：TCP、WebSocket、QUIC v1、WebTransport (QUIC)、WebRTC Direct
// 安全传输：Noise、TLS
// 多路复用：Yamux
// NAT 穿透：Hole Punching、AutoRelay、AutoNAT v2
// 局域网发现：mDNS
//
// 如果未提供 PrivateKey，自动生成 Ed25519 密钥对。
func NewHost(cfg HostConfig) (host.Host, error) {
	if len(cfg.ListenAddresses) == 0 {
		cfg.ListenAddresses = DefaultHostConfig().ListenAddresses
	}

	// 1. 生成或解析密钥对
	privKey, err := generateOrParseKey(cfg.PrivateKey, cfg.KeyPath)
	if err != nil {
		return nil, fmt.Errorf("生成密钥对失败: %w", err)
	}

	// 2. 解析多地址
	listenAddrs := make([]multiaddr.Multiaddr, 0, len(cfg.ListenAddresses))
	for _, addrStr := range cfg.ListenAddresses {
		addr, err := multiaddr.NewMultiaddr(addrStr)
		if err != nil {
			return nil, fmt.Errorf("解析监听地址 %q 失败: %w", addrStr, err)
		}
		listenAddrs = append(listenAddrs, addr)
	}

	// 3. 连接管理器
	connMgr, err := connmgr.NewConnManager(
		cfg.ConnMgrLow,
		cfg.ConnMgrHigh,
		connmgr.WithGracePeriod(cfg.ConnMgrGrace),
	)
	if err != nil {
		return nil, fmt.Errorf("创建连接管理器失败: %w", err)
	}

	// 4. 构建 libp2p 选项
	opts := []libp2p.Option{
		libp2p.Identity(privKey),
		libp2p.ListenAddrs(listenAddrs...),
		libp2p.ConnectionManager(connMgr),
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Transport(websocket.New),
		libp2p.Transport(quic.NewTransport),
		libp2p.Transport(webtransport.New),
		libp2p.Transport(libp2pwebrtc.New),
		libp2p.Security(noise.ID, noise.New),
		libp2p.Security(tls.ID, tls.New),
		libp2p.ShareTCPListener(),
		libp2p.UserAgent("IPFAR/"+version.Short()),
		libp2p.ProtocolVersion("ipfar/1.0.0"),
	}

	// NAT 端口映射 (UPnP / NAT-PMP)
	if cfg.EnableNATPortMap {
		opts = append(opts, libp2p.NATPortMap())
	}

	// Relay (Circuit Relay v2)
	if cfg.EnableRelay {
		opts = append(opts,
			libp2p.EnableRelay(),
			libp2p.EnableRelayService(),
		)
	}

	// AutoRelay — 自动发现中继服务器
	if cfg.EnableAutoRelay {
		opts = append(opts, libp2p.EnableAutoRelay())
	}

	// Hole Punching — NAT 打洞
	if cfg.EnableHolePunching {
		opts = append(opts, libp2p.EnableHolePunching())
	}

	// AutoNAT v2 — 更高效的 NAT 检测（优先于 v1）
	if cfg.EnableAutoNATv2 {
		opts = append(opts, libp2p.EnableAutoNATv2())
	} else if cfg.EnableAutoNAT {
		// AutoNAT v1（仅在 v2 未启用时生效）
		opts = append(opts, libp2p.EnableNATService())
	}

	// 资源管理器
	if cfg.ResourceManager != nil {
		opts = append(opts, libp2p.ResourceManager(cfg.ResourceManager))
	} else {
		concrete := rcmgr.DefaultLimits.AutoScale()
		limiter := rcmgr.NewFixedLimiter(concrete)
		rm, err := rcmgr.NewResourceManager(limiter)
		if err == nil {
			opts = append(opts, libp2p.ResourceManager(rm))
		}
	}

	// 5. 创建 host
	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("创建 libp2p host 失败: %w", err)
	}

	pid := h.ID()
	log.Info("DHT Host: libp2p host 已创建 peer_id=%s", pid)

	// 6. 启动 mDNS 局域网发现
	mdnsSer := mdns.NewMdnsService(h, "ipfar-bridge", &mdnsNotifee{host: h})
	if err := mdnsSer.Start(); err != nil {
		log.Warn("DHT Host: mDNS 服务启动失败: %v", err)
	} else {
		log.Info("DHT Host: mDNS 局域网发现已启动")
	}

	// 7. 连接到引导节点
	if len(cfg.BootstrapPeers) > 0 {
		go connectToBootstrapPeers(h, cfg.BootstrapPeers)
	}

	// 8. 打印监听地址
	for _, addr := range h.Addrs() {
		fullAddr, _ := multiaddr.NewMultiaddr(fmt.Sprintf("%s/p2p/%s", addr, pid))
		if fullAddr != nil {
			log.Info("DHT Host: 监听地址 %s", fullAddr)
		}
	}

	return h, nil
}

// NewProviderWithHost 便捷函数：创建 host 并初始化 DHT Provider
//
// 组合 NewHost 和 NewProvider，一次性完成 host 和 provider 的创建与启动。
// provider 创建后会自动调用 Start()。
func NewProviderWithHost(hostCfg HostConfig, providerCfg Config) (*Provider, host.Host, error) {
	h, err := NewHost(hostCfg)
	if err != nil {
		return nil, nil, fmt.Errorf("创建 host 失败: %w", err)
	}

	provider, err := NewProvider(h, providerCfg)
	if err != nil {
		h.Close()
		return nil, nil, fmt.Errorf("创建 Provider 失败: %w", err)
	}

	if err := provider.Start(); err != nil {
		h.Close()
		return nil, nil, fmt.Errorf("启动 Provider 失败: %w", err)
	}

	return provider, h, nil
}

// generateOrParseKey 生成 Ed25519 密钥对，或从 PEM 字符串/文件中加载已有私钥。
//
// 优先级：pemKey > keyPath 文件 > 生成新密钥
// 如果 keyPath 不为空且文件存在，从 PEM 文件加载；不存在则生成并持久化到 keyPath。
func generateOrParseKey(pemKey string, keyPath string) (crypto.PrivKey, error) {
	// 1. 优先使用 PEM 字符串
	if pemKey != "" {
		privKey, err := crypto.UnmarshalPrivateKey([]byte(pemKey))
		if err != nil {
			return nil, fmt.Errorf("解析 PEM 私钥失败: %w", err)
		}
		return privKey, nil
	}

	// 2. 从文件加载或生成持久化密钥
	if keyPath != "" {
		return loadOrGenerateKeyFile(keyPath)
	}

	// 3. 生成临时密钥（不持久化）
	return generateEd25519Key()
}

// loadOrGenerateKeyFile 从 PEM 文件加载 Ed25519 私钥，不存在则生成并写入文件
func loadOrGenerateKeyFile(keyPath string) (crypto.PrivKey, error) {
	data, err := os.ReadFile(keyPath)
	if err == nil {
		// 文件存在，解析 PEM
		block, _ := pem.Decode(data)
		if block == nil {
			return nil, fmt.Errorf("解析密钥文件 %s 失败: 无效的 PEM 格式", keyPath)
		}

		parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("解析 PKCS8 私钥失败 %s: %w", keyPath, err)
		}

		edKey, ok := parsed.(ed25519.PrivateKey)
		if !ok {
			return nil, fmt.Errorf("密钥文件 %s 不是 Ed25519 私钥", keyPath)
		}

		privKey, err := crypto.UnmarshalEd25519PrivateKey(edKey)
		if err != nil {
			return nil, fmt.Errorf("转换 Ed25519 私钥失败: %w", err)
		}

		log.Info("DHT Host: 已从文件加载节点私钥 path=%s", keyPath)
		return privKey, nil
	}

	if !os.IsNotExist(err) {
		return nil, fmt.Errorf("读取密钥文件 %s 失败: %w", keyPath, err)
	}

	// 文件不存在，生成新密钥并持久化
	_, rawPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("生成 Ed25519 密钥对失败: %w", err)
	}

	// 使用 PKCS#8 + PEM 格式持久化
	derBytes, err := x509.MarshalPKCS8PrivateKey(rawPriv)
	if err != nil {
		return nil, fmt.Errorf("序列化 PKCS8 私钥失败: %w", err)
	}

	pemBlock := &pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: derBytes,
	}
	pemData := pem.EncodeToMemory(pemBlock)

	if err := os.WriteFile(keyPath, pemData, 0600); err != nil {
		return nil, fmt.Errorf("写入密钥文件 %s 失败: %w", keyPath, err)
	}

	log.Info("DHT Host: 已生成并持久化节点私钥到 path=%s", keyPath)

	privKey, err := crypto.UnmarshalEd25519PrivateKey(rawPriv)
	if err != nil {
		return nil, fmt.Errorf("转换 Ed25519 私钥失败: %w", err)
	}

	return privKey, nil
}

// generateEd25519Key 生成临时 Ed25519 密钥（不持久化）
func generateEd25519Key() (crypto.PrivKey, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("生成 Ed25519 密钥对失败: %w", err)
	}

	privKey, err := crypto.UnmarshalEd25519PrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("转换私钥失败: %w", err)
	}
	_ = pub // 公钥由 libp2p 从私钥派生

	return privKey, nil
}

// mdnsNotifee 实现 mdns.Notifee 接口，用于处理 mDNS 发现的节点
type mdnsNotifee struct {
	host host.Host
}

// HandlePeerFound 当 mDNS 发现对等节点时被调用
func (n *mdnsNotifee) HandlePeerFound(pi peer.AddrInfo) {
	log.Info("mDNS: 发现对等节点 %s, 地址 %v", pi.ID, pi.Addrs)
	if err := n.host.Connect(context.Background(), pi); err != nil {
		log.Debug("mDNS: 连接 %s 失败: %v", pi.ID, err)
	}
}

// connectToBootstrapPeers 连接到引导节点列表
func connectToBootstrapPeers(h host.Host, peers []string) {
	var connected int
	for _, addrStr := range peers {
		addr, err := multiaddr.NewMultiaddr(addrStr)
		if err != nil {
			log.Warn("DHT Host: 无效的引导节点地址 %q: %v", addrStr, err)
			continue
		}

		info, err := peer.AddrInfoFromP2pAddr(addr)
		if err != nil {
			log.Warn("DHT Host: 无法解析引导节点地址 %q: %v", addrStr, err)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := h.Connect(ctx, *info); err != nil {
			log.Warn("DHT Host: 连接引导节点失败 %q: %v", addrStr, err)
			cancel()
			continue
		}
		cancel()

		connected++
		log.Info("DHT Host: 已连接到引导节点 %s", info.ID)
	}

	log.Info("DHT Host: 引导完成 connected=%d/%d", connected, len(peers))
}
