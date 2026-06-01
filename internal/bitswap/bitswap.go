// Package bitswap 提供 IPFS Bitswap 协议的轻量级实现
//
// 该模块直接实现 Bitswap 线协议 (v1.2.0)，无需依赖完整的 boxo/libp2p 生态，
// 在 1.6G 内存约束下保持精简。
//
// 功能：
//   - Bitswap 客户端/服务端（支持裸 TCP 和 libp2p host 两种模式）
//   - 延迟回复机制：降低本桥节点在 IPFS 网络中的回源优先级
//   - 请求到的 block 自动写入本地缓存
//   - WantList 管理 + Block 收发
//
// 协议参考: https://github.com/ipfs/specs/blob/main/network-protocols/bitswap.md
// 规范参考: ipfar-specs/V1/项目规划.md §四.1、§四.2
package bitswap

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	cid "github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-varint"

	"github.com/lwdjd/IPFAR/internal/cache"
	"github.com/lwdjd/IPFAR/internal/log"
)

// ============================================================
// BlockFetcher 接口
// ============================================================

// BlockFetcher 按需获取 IPFS 块的接口
// 当 Bitswap 收到 WantList 且本地缓存未命中时，调用此接口按需获取数据。
type BlockFetcher interface {
	FetchBlock(ctx context.Context, cid string) ([]byte, error)
}

// ============================================================
// 内部抽象：连接接口
// ============================================================

// connLike 抽象 net.Conn 和 network.Stream，使核心协议逻辑对传输层透明。
type connLike interface {
	io.Reader
	io.Writer
	io.Closer
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
}

// ============================================================
// 常量
// ============================================================

const (
	// Bitswap 协议 ID (v1.2.0)
	protocolID = "/ipfs/bitswap/1.2.0"

	// 消息类型
	msgWantList = 0
	msgBlock    = 1
	msgHave     = 2
	msgDontHave = 3

	// 默认配置
	defaultTimeout       = 30 * time.Second
	defaultMaxMsgSize    = 4 * 1024 * 1024 // 4 MB
	defaultDelayDuration = 2 * time.Second
)

// ============================================================
// 配置
// ============================================================

// Config Bitswap 服务配置
type Config struct {
	// ListenAddr TCP 监听地址，为空则不启动服务端。
	// 在共享 host 模式（NewWithHost）下此字段被忽略。
	ListenAddr string
	// DelayedReply 是否启用延迟回复（推荐开启）
	DelayedReply bool
	// DelayDuration 延迟回复时长
	DelayDuration time.Duration
	// Cache 本地缓存实例
	Cache *cache.Cache
	// CacheConfig 若 Cache 为 nil，用此配置自动创建
	CacheConfig cache.CacheConfig
	// Timeout 单次请求超时
	Timeout time.Duration
	// MaxMsgSize 最大消息大小
	MaxMsgSize int64
}

// DefaultConfig 默认 Bitswap 配置
func DefaultConfig() Config {
	return Config{
		ListenAddr:    ":4001",
		DelayedReply:  true,
		DelayDuration: 2 * time.Second,
		CacheConfig:   cache.DefaultCacheConfig(),
		Timeout:       30 * time.Second,
		MaxMsgSize:    4 * 1024 * 1024,
	}
}

// ============================================================
// Service
// ============================================================

// Service Bitswap 服务
type Service struct {
	config Config

	mu     sync.RWMutex
	cache  *cache.Cache

	// TCP 模式字段
	listener net.Listener

	// libp2p 共享 host 模式字段
	host      host.Host
	streamCtx context.Context
	streamWG  sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Peer 连接管理
	peers   map[string]*peerConn
	peersMu sync.Mutex

	// WantList: key=CID string, value=引用计数
	wantList   map[string]int32
	wantListMu sync.Mutex

	// BlockFetcher 按需拉取器（可选）
	// 设置后，当 WantList 中请求的块不在本地缓存中时，
	// Bitswap 会调用 blockFetcher.FetchBlock() 按需从 Arweave 获取。
	blockFetcher   BlockFetcher
	blockFetcherMu sync.RWMutex

	// 统计
	blocksRequested uint64
	blocksServed    uint64
	blocksCached    uint64
}

// peerConn 代表一个 peer 连接
type peerConn struct {
	conn      connLike
	addr      string // TCP 地址 或 peer.ID 字符串
	writeMu   sync.Mutex
	closeOnce sync.Once
	closed    chan struct{}
}

// ============================================================
// 构造函数
// ============================================================

// New 创建 Bitswap 服务（裸 TCP 模式，独立监听端口）
func New(config Config) (*Service, error) {
	ctx, cancel := context.WithCancel(context.Background())

	if config.Timeout <= 0 {
		config.Timeout = defaultTimeout
	}
	if config.MaxMsgSize <= 0 {
		config.MaxMsgSize = defaultMaxMsgSize
	}

	s := &Service{
		config:   config,
		ctx:      ctx,
		cancel:   cancel,
		peers:    make(map[string]*peerConn),
		wantList: make(map[string]int32),
	}

	// 初始化缓存
	if config.Cache != nil {
		s.cache = config.Cache
	} else {
		var err error
		s.cache, err = cache.New(config.CacheConfig)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("bitswap: 创建缓存失败: %w", err)
		}
	}

	// 启动 TCP 监听
	if config.ListenAddr != "" {
		ln, err := net.Listen("tcp", config.ListenAddr)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("bitswap: 监听失败 %s: %w", config.ListenAddr, err)
		}
		s.listener = ln

		s.wg.Add(1)
		go s.acceptLoop()

		log.Info("Bitswap：监听 %s（裸 TCP 模式）", ln.Addr().String())
	}

	log.Info("Bitswap：服务已初始化 delayed=%v delay=%v mode=tcp",
		config.DelayedReply, config.DelayDuration)
	return s, nil
}

// NewWithHost 创建 Bitswap 服务（共享 libp2p host 模式）
//
// Bitswap 会在 host 上注册 stream handler（协议 /ipfs/bitswap/1.2.0），
// 不再自己监听 TCP 端口。出站连接通过 host.NewStream 发起。
// host 的生命周期由调用者管理，Close() 不会关闭 host。
func NewWithHost(host host.Host, config Config) (*Service, error) {
	ctx, cancel := context.WithCancel(context.Background())

	if config.Timeout <= 0 {
		config.Timeout = defaultTimeout
	}
	if config.MaxMsgSize <= 0 {
		config.MaxMsgSize = defaultMaxMsgSize
	}

	s := &Service{
		config:   config,
		host:     host,
		ctx:      ctx,
		cancel:   cancel,
		peers:    make(map[string]*peerConn),
		wantList: make(map[string]int32),
	}

	// 初始化缓存
	if config.Cache != nil {
		s.cache = config.Cache
	} else {
		var err error
		s.cache, err = cache.New(config.CacheConfig)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("bitswap: 创建缓存失败: %w", err)
		}
	}

	// 在 host 上注册 stream handler（libp2p 自动处理 multistream-select 协商）
	host.SetStreamHandler(protocol.ID(protocolID), s.handleStream)

	log.Info("Bitswap：服务已初始化 delayed=%v delay=%v mode=shared-host peer_id=%s",
		config.DelayedReply, config.DelayDuration, host.ID())
	return s, nil
}

// ============================================================
// 生命周期
// ============================================================

// Close 关闭服务
func (s *Service) Close() error {
	s.cancel()

	// libp2p 模式：取消 stream handler 注册
	if s.host != nil {
		s.host.RemoveStreamHandler(protocol.ID(protocolID))
	}

	// TCP 模式：关闭监听器
	if s.listener != nil {
		s.listener.Close()
	}

	// 关闭所有 peer 连接
	s.peersMu.Lock()
	for _, pc := range s.peers {
		pc.close()
	}
	s.peersMu.Unlock()

	s.wg.Wait()
	s.streamWG.Wait()

	if s.cache != nil {
		s.cache.Close()
	}

	log.Info("Bitswap：服务已关闭")
	return nil
}

// ============================================================
// 公共 API
// ============================================================

// GetBlock 向指定 peer 请求一个 block
//
// 在 TCP 模式下，peerAddr 为 "host:port" 格式。
// 在共享 host 模式下，peerAddr 为包含 peer ID 的 multiaddr 字符串
// （如 "/ip4/1.2.3.4/tcp/4001/p2p/12D3KooW..."）。
func (s *Service) GetBlock(ctx context.Context, peerAddr string, c cid.Cid) ([]byte, error) {
	atomic.AddUint64(&s.blocksRequested, 1)

	// 先查缓存
	if data, found, _ := s.cache.Get(c.String()); found {
		return data, nil
	}

	// 建立连接
	pc, err := s.connect(peerAddr)
	if err != nil {
		return nil, fmt.Errorf("bitswap: connect to %s: %w", peerAddr, err)
	}

	// 发送 WantList
	if err := s.sendWantList(pc, []cid.Cid{c}); err != nil {
		return nil, fmt.Errorf("bitswap: send wantlist: %w", err)
	}

	// 等待响应
	deadline := time.Now().Add(s.config.Timeout)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	pc.conn.SetReadDeadline(deadline)

	data, err := s.readBlockResponse(pc, c)
	if err != nil {
		return nil, fmt.Errorf("bitswap: read response: %w", err)
	}

	// 缓存
	s.cache.Put(c.String(), data)
	atomic.AddUint64(&s.blocksCached, 1)

	return data, nil
}

// FetchBlock 通过 libp2p host 向指定 peer 请求一个 block
//
// 这是共享 host 模式下的原生出站方法。内部使用 host.NewStream 创建连接，
// libp2p 自动处理 multistream-select 协议协商。
func (s *Service) FetchBlock(ctx context.Context, peerID peer.ID, c cid.Cid) ([]byte, error) {
	atomic.AddUint64(&s.blocksRequested, 1)

	// 先查缓存
	if data, found, _ := s.cache.Get(c.String()); found {
		return data, nil
	}

	// 通过 libp2p 建立 stream
	pc, err := s.connectPeer(ctx, peerID)
	if err != nil {
		return nil, fmt.Errorf("bitswap: connect to peer %s: %w", peerID, err)
	}

	// 发送 WantList
	if err := s.sendWantList(pc, []cid.Cid{c}); err != nil {
		return nil, fmt.Errorf("bitswap: send wantlist: %w", err)
	}

	// 等待响应
	deadline := time.Now().Add(s.config.Timeout)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	pc.conn.SetReadDeadline(deadline)

	data, err := s.readBlockResponse(pc, c)
	if err != nil {
		return nil, fmt.Errorf("bitswap: read response: %w", err)
	}

	// 缓存
	s.cache.Put(c.String(), data)
	atomic.AddUint64(&s.blocksCached, 1)

	return data, nil
}

// HasBlock 检查本地是否拥有某个 block
func (s *Service) HasBlock(c cid.Cid) bool {
	return s.cache.Has(c.String())
}

// Cache 返回内部缓存实例
func (s *Service) Cache() *cache.Cache {
	return s.cache
}

// ListenAddr 返回监听地址
func (s *Service) ListenAddr() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	if s.host != nil {
		addrs := s.host.Addrs()
		if len(addrs) > 0 {
			return addrs[0].String() + "/p2p/" + s.host.ID().String()
		}
		return s.host.ID().String()
	}
	return ""
}

// Host 返回共享的 libp2p host（仅在 NewWithHost 模式下非 nil）
func (s *Service) Host() host.Host {
	return s.host
}

// Stats 返回统计信息
func (s *Service) Stats() Stats {
	return Stats{
		ListenAddr:      s.ListenAddr(),
		BlocksRequested: atomic.LoadUint64(&s.blocksRequested),
		BlocksServed:    atomic.LoadUint64(&s.blocksServed),
		BlocksCached:    atomic.LoadUint64(&s.blocksCached),
		CacheStats:      s.cache.Stats(),
	}
}

// SetBlockFetcher 设置按需拉取器
// 设置后，当 WantList 中请求的块不在本地缓存中时，
// Bitswap 会调用 blockFetcher.FetchBlock() 按需从 Arweave 获取。
func (s *Service) SetBlockFetcher(fetcher BlockFetcher) {
	s.blockFetcherMu.Lock()
	defer s.blockFetcherMu.Unlock()
	s.blockFetcher = fetcher
	log.Info("Bitswap：BlockFetcher 已注册")
}

// GetBlockFetcher 获取按需拉取器
func (s *Service) GetBlockFetcher() BlockFetcher {
	s.blockFetcherMu.RLock()
	defer s.blockFetcherMu.RUnlock()
	return s.blockFetcher
}

// ============================================================
// 连接管理 (TCP 模式)
// ============================================================

// connect 建立到指定地址的连接（TCP 模式使用 net.Dial；共享 host 模式解析 multiaddr）
func (s *Service) connect(addr string) (*peerConn, error) {
	s.peersMu.Lock()
	if pc, ok := s.peers[addr]; ok {
		// 检查连接是否仍然存活
		select {
		case <-pc.closed:
			delete(s.peers, addr)
		default:
			s.peersMu.Unlock()
			return pc, nil
		}
	}
	s.peersMu.Unlock()

	// 共享 host 模式：解析 multiaddr 后通过 host.NewStream 连接
	if s.host != nil {
		return s.connectViaMultiaddr(addr)
	}

	// TCP 模式：裸 TCP 连接
	conn, err := net.DialTimeout("tcp", addr, s.config.Timeout)
	if err != nil {
		return nil, err
	}

	// 发送协议握手
	if err := s.handshake(conn); err != nil {
		conn.Close()
		return nil, err
	}

	pc := &peerConn{
		conn:   conn,
		addr:   addr,
		closed: make(chan struct{}),
	}

	s.peersMu.Lock()
	s.peers[addr] = pc
	s.peersMu.Unlock()

	return pc, nil
}

// connectViaMultiaddr 解析 multiaddr 地址并使用共享 host 建立 libp2p stream
func (s *Service) connectViaMultiaddr(addr string) (*peerConn, error) {
	maddr, err := multiaddr.NewMultiaddr(addr)
	if err != nil {
		return nil, fmt.Errorf("无效的 multiaddr %q: %w", addr, err)
	}

	info, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		return nil, fmt.Errorf("无法从 multiaddr 提取 peer 信息 %q: %w", addr, err)
	}

	return s.connectPeer(context.Background(), info.ID)
}

// connectPeer 通过共享 host 建立到指定 peer 的 libp2p stream
func (s *Service) connectPeer(ctx context.Context, peerID peer.ID) (*peerConn, error) {
	if s.host == nil {
		return nil, fmt.Errorf("bitswap: host 为 nil，无法建立 libp2p 连接（请使用 NewWithHost 初始化）")
	}

	pidStr := peerID.String()

	s.peersMu.Lock()
	if pc, ok := s.peers[pidStr]; ok {
		select {
		case <-pc.closed:
			delete(s.peers, pidStr)
		default:
			s.peersMu.Unlock()
			return pc, nil
		}
	}
	s.peersMu.Unlock()

	stream, err := s.host.NewStream(ctx, peerID, protocol.ID(protocolID))
	if err != nil {
		return nil, fmt.Errorf("创建 stream 到 peer %s: %w", peerID, err)
	}

	pc := &peerConn{
		conn:   stream,
		addr:   pidStr,
		closed: make(chan struct{}),
	}

	s.peersMu.Lock()
	s.peers[pidStr] = pc
	s.peersMu.Unlock()

	log.Debug("Bitswap：已连接到 peer %s", peerID)
	return pc, nil
}

func (pc *peerConn) close() {
	pc.closeOnce.Do(func() {
		close(pc.closed)
		pc.conn.Close()
	})
}

func (pc *peerConn) write(data []byte) error {
	pc.writeMu.Lock()
	defer pc.writeMu.Unlock()
	_, err := pc.conn.Write(data)
	return err
}

// ============================================================
// 协议实现
// ============================================================

// handshake 发送多流协议握手（仅 TCP 模式使用；libp2p 模式由 multistream-select 自动协商）
func (s *Service) handshake(conn net.Conn) error {
	// Multistream select: 发送协议 ID
	// 格式: varint(协议ID长度) + 协议ID字节 + "\n"
	protoBytes := []byte(protocolID)
	buf := make([]byte, 0, 8+len(protoBytes)+1)
	buf = append(buf, varint.ToUvarint(uint64(len(protoBytes)))...)
	buf = append(buf, protoBytes...)
	buf = append(buf, '\n')

	if _, err := conn.Write(buf); err != nil {
		return fmt.Errorf("handshake write: %w", err)
	}

	// 读取对方响应
	resp := make([]byte, 1024)
	n, err := conn.Read(resp)
	if err != nil {
		return fmt.Errorf("handshake read: %w", err)
	}

	// 期望响应为相同的协议 ID 或 "na\n"
	respStr := string(resp[:n])
	if respStr == "na\n" {
		return fmt.Errorf("protocol not supported by peer")
	}

	return nil
}

// sendWantList 发送 WantList 消息
func (s *Service) sendWantList(pc *peerConn, cids []cid.Cid) error {
	var buf bytes.Buffer

	// 消息头: varint(类型)
	buf.WriteByte(msgWantList)

	// WantList payload: varint(count) + for each: varint(cid_len) + cid_bytes + varint(priority)
	buf.Write(varint.ToUvarint(uint64(len(cids))))
	for _, c := range cids {
		cidBytes := c.Bytes()
		buf.Write(varint.ToUvarint(uint64(len(cidBytes))))
		buf.Write(cidBytes)
		buf.Write(varint.ToUvarint(1)) // priority = 1 (normal)
	}

	return pc.write(buf.Bytes())
}

// readBlockResponse 读取 Block 响应
func (s *Service) readBlockResponse(pc *peerConn, expected cid.Cid) ([]byte, error) {
	for {
		msgType, err := s.readVarint(pc.conn)
		if err != nil {
			return nil, fmt.Errorf("read msg type: %w", err)
		}

		switch msgType {
		case msgBlock:
			// Block: varint(cid_len) + cid_bytes + varint(data_len) + data
			cidLen, err := s.readVarint(pc.conn)
			if err != nil {
				return nil, fmt.Errorf("read cid len: %w", err)
			}
			cidBytes := make([]byte, cidLen)
			if _, err := io.ReadFull(pc.conn, cidBytes); err != nil {
				return nil, fmt.Errorf("read cid: %w", err)
			}

			dataLen, err := s.readVarint(pc.conn)
			if err != nil {
				return nil, fmt.Errorf("read data len: %w", err)
			}
			if dataLen > uint64(s.config.MaxMsgSize) {
				return nil, fmt.Errorf("block too large: %d bytes", dataLen)
			}

			data := make([]byte, dataLen)
			if _, err := io.ReadFull(pc.conn, data); err != nil {
				return nil, fmt.Errorf("read data: %w", err)
			}

			recvCID, err := cid.Cast(cidBytes)
			if err != nil {
				return nil, fmt.Errorf("invalid received cid: %w", err)
			}

			if !recvCID.Equals(expected) {
				log.Debug("Bitswap: received unexpected cid %s (expected %s)", recvCID, expected)
				continue
			}

			return data, nil

		case msgHave:
			// 对方有该 block，但没发送（可能是中继场景），继续等待
			_, err := s.readVarint(pc.conn) // cid_len
			if err != nil {
				return nil, fmt.Errorf("read have cid_len: %w", err)
			}
			// 跳过 cid
			continue

		case msgDontHave:
			// 对方没有该 block
			_, err := s.readVarint(pc.conn) // cid_len
			if err != nil {
				return nil, fmt.Errorf("read donthave cid_len: %w", err)
			}
			return nil, fmt.Errorf("peer doesn't have block %s", expected)

		default:
			return nil, fmt.Errorf("unexpected message type: %d", msgType)
		}
	}
}

// ============================================================
// 服务端 - TCP 模式 (accept loop)
// ============================================================

func (s *Service) acceptLoop() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				return
			default:
				log.Warn("Bitswap：accept 错误: %v", err)
				continue
			}
		}

		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

func (s *Service) handleConn(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	// 读取握手
	buf := make([]byte, 256)
	n, err := conn.Read(buf)
	if err != nil {
		return
	}

	// 验证协议
	protoLen, nRead := binary.Uvarint(buf[:n])
	if nRead <= 0 || int(protoLen)+nRead > n {
		return
	}
	protoStr := string(buf[nRead : nRead+int(protoLen)])
	if protoStr != protocolID {
		conn.Write([]byte("na\n"))
		return
	}

	// 回复握手确认
	conn.Write(buf[:n])

	// 处理请求
	s.processMessages(conn)
}

// ============================================================
// 服务端 - libp2p 共享 host 模式 (stream handler)
// ============================================================

// handleStream 处理来自 libp2p host 的入站 stream
//
// libp2p 已通过 multistream-select 完成协议协商，stream 就绪后直接处理 Bitswap 消息。
func (s *Service) handleStream(stream network.Stream) {
	s.streamWG.Add(1)
	defer s.streamWG.Done()
	defer stream.Close()

	log.Debug("Bitswap：收到来自 peer %s 的 stream", stream.Conn().RemotePeer())

	// libp2p 已协商协议，直接处理消息
	s.processMessages(stream)
}

// processMessages 处理连接上的 Bitswap 消息（TCP 和 libp2p 共用）
func (s *Service) processMessages(conn connLike) {
	for {
		msgType, err := s.readVarint(conn)
		if err != nil {
			return
		}

		switch msgType {
		case msgWantList:
			s.handleWantList(conn)
		default:
			log.Debug("Bitswap: 未知消息类型: %d", msgType)
			return
		}
	}
}

func (s *Service) handleWantList(conn connLike) {
	// 读取 WantList
	count, err := s.readVarint(conn)
	if err != nil {
		return
	}

	var cids []cid.Cid
	for i := uint64(0); i < count; i++ {
		cidLen, err := s.readVarint(conn)
		if err != nil {
			return
		}
		if cidLen > 256 {
			return
		}
		cidBytes := make([]byte, cidLen)
		if _, err := io.ReadFull(conn, cidBytes); err != nil {
			return
		}
		_, err = s.readVarint(conn) // priority, skip
		if err != nil {
			return
		}

		c, err := cid.Cast(cidBytes)
		if err != nil {
			continue
		}
		cids = append(cids, c)
	}

	if len(cids) == 0 {
		return
	}

	// 延迟回复机制
	if s.config.DelayedReply && s.config.DelayDuration > 0 {
		timer := time.NewTimer(s.config.DelayDuration)
		select {
		case <-s.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}

	// 回复每个请求的 CID
	for _, c := range cids {
		cidStr := c.String()

		// 1. 先查本地缓存
		data, found, _ := s.cache.Get(cidStr)
		if found {
			s.sendBlock(conn, c, data)
			atomic.AddUint64(&s.blocksServed, 1)
			continue
		}

		// 2. 缓存未命中 → 尝试按需拉取
		fetcher := s.GetBlockFetcher()
		if fetcher != nil {
			log.Debug("Bitswap：缓存未命中 %s，尝试按需拉取...", cidStr)

			fetchCtx, cancel := context.WithTimeout(s.ctx, s.config.Timeout)
			fetchedData, err := fetcher.FetchBlock(fetchCtx, cidStr)
			cancel()

			if err != nil {
				log.Warn("Bitswap：按需拉取失败 %s: %v", cidStr, err)
				s.sendDontHave(conn, c)
				continue
			}

			// 写入本地缓存
			s.cache.Put(cidStr, fetchedData)
			atomic.AddUint64(&s.blocksCached, 1)

			// 发送块
			s.sendBlock(conn, c, fetchedData)
			atomic.AddUint64(&s.blocksServed, 1)
			log.Info("Bitswap：按需拉取成功 %s (%d bytes)", cidStr, len(fetchedData))
			continue
		}

		// 3. 没有 BlockFetcher → 回复 dont-have
		s.sendDontHave(conn, c)
	}
}

func (s *Service) sendBlock(w io.Writer, c cid.Cid, data []byte) error {
	var buf bytes.Buffer
	buf.WriteByte(msgBlock)

	cidBytes := c.Bytes()
	buf.Write(varint.ToUvarint(uint64(len(cidBytes))))
	buf.Write(cidBytes)
	buf.Write(varint.ToUvarint(uint64(len(data))))
	buf.Write(data)

	_, err := w.Write(buf.Bytes())
	return err
}

func (s *Service) sendDontHave(w io.Writer, c cid.Cid) error {
	var buf bytes.Buffer
	buf.WriteByte(msgDontHave)

	cidBytes := c.Bytes()
	buf.Write(varint.ToUvarint(uint64(len(cidBytes))))
	buf.Write(cidBytes)

	_, err := w.Write(buf.Bytes())
	return err
}

// ============================================================
// 工具函数
// ============================================================

func (s *Service) readVarint(r io.Reader) (uint64, error) {
	var buf [1]byte
	var result uint64
	var shift uint

	for {
		_, err := io.ReadFull(r, buf[:])
		if err != nil {
			return 0, err
		}
		b := buf[0]
		result |= uint64(b&0x7F) << shift
		if b&0x80 == 0 {
			break
		}
		shift += 7
		if shift >= 64 {
			return 0, fmt.Errorf("varint overflow")
		}
	}
	return result, nil
}

// ============================================================
// 类型
// ============================================================

// Stats Bitswap 统计信息
type Stats struct {
	ListenAddr      string           `json:"listen_addr"`
	BlocksRequested uint64           `json:"blocks_requested"`
	BlocksServed    uint64           `json:"blocks_served"`
	BlocksCached    uint64           `json:"blocks_cached"`
	CacheStats      cache.CacheStats `json:"cache_stats"`
}
