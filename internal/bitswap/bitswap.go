// Package bitswap 提供 IPFS Bitswap 协议的轻量级实现
//
// 该模块直接实现 Bitswap 线协议 (v1.2.0)，无需依赖完整的 boxo/libp2p 生态，
// 在 1.6G 内存约束下保持精简。
//
// 功能：
//   - Bitswap 客户端/服务端 TCP 直连
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
	"github.com/multiformats/go-varint"

	"github.com/lwdjd/IPFAR/internal/cache"
	"github.com/lwdjd/IPFAR/internal/log"
)

// ============================================================
// 常量
// ============================================================

const (
	// Bitswap 协议 ID (v1.2.0)
	protocolID = "/ipfs/bitswap/1.2.0"

	// 消息类型
	msgWantList  = 0
	msgBlock     = 1
	msgHave      = 2
	msgDontHave  = 3

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
	// ListenAddr TCP 监听地址，为空则不启动服务端
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

	listener   net.Listener
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup

	// Peer 连接管理
	peers   map[string]*peerConn
	peersMu sync.Mutex

	// WantList: key=CID string, value=引用计数
	wantList   map[string]int32
	wantListMu sync.Mutex

	// 统计
	blocksRequested uint64
	blocksServed    uint64
	blocksCached    uint64
}

// peerConn 代表一个 peer 连接
type peerConn struct {
	conn      net.Conn
	addr      string
	writeMu   sync.Mutex
	closeOnce sync.Once
	closed    chan struct{}
}

// New 创建 Bitswap 服务
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

		log.Info("Bitswap：监听 %s", ln.Addr().String())
	}

	log.Info("Bitswap：服务已初始化 delayed=%v delay=%v",
		config.DelayedReply, config.DelayDuration)
	return s, nil
}

// Close 关闭服务
func (s *Service) Close() error {
	s.cancel()

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
	return ""
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

// ============================================================
// 连接管理
// ============================================================

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

// handshake 发送多流协议握手
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
// 服务端 (accept loop)
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

func (s *Service) handleWantList(conn net.Conn) {
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
		data, found, _ := s.cache.Get(c.String())
		if found {
			s.sendBlock(conn, c, data)
			atomic.AddUint64(&s.blocksServed, 1)
		} else {
			s.sendDontHave(conn, c)
		}
	}
}

func (s *Service) sendBlock(conn net.Conn, c cid.Cid, data []byte) error {
	var buf bytes.Buffer
	buf.WriteByte(msgBlock)

	cidBytes := c.Bytes()
	buf.Write(varint.ToUvarint(uint64(len(cidBytes))))
	buf.Write(cidBytes)
	buf.Write(varint.ToUvarint(uint64(len(data))))
	buf.Write(data)

	_, err := conn.Write(buf.Bytes())
	return err
}

func (s *Service) sendDontHave(conn net.Conn, c cid.Cid) error {
	var buf bytes.Buffer
	buf.WriteByte(msgDontHave)

	cidBytes := c.Bytes()
	buf.Write(varint.ToUvarint(uint64(len(cidBytes))))
	buf.Write(cidBytes)

	_, err := conn.Write(buf.Bytes())
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
	ListenAddr      string          `json:"listen_addr"`
	BlocksRequested uint64          `json:"blocks_requested"`
	BlocksServed    uint64          `json:"blocks_served"`
	BlocksCached    uint64          `json:"blocks_cached"`
	CacheStats      cache.CacheStats `json:"cache_stats"`
}
