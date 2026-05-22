// Package discovery 提供 IPFAR 数据发现功能
//
// GraphQL 模式：通过 Arweave GraphQL API 按 Protocol Tag 搜索元数据交易。
// 相比随机抽样模式，GraphQL 模式可精确定位包含 IPFAR 数据的区块。
//
// 规范参考: ipfar-specs/V1/项目规划.md §3.1

package discovery

import (
	"context"
	"encoding/base64"
	"fmt"
	"time"

	sdkArweave "github.com/LWDJD/ipfar-sdk/arweave"

	"github.com/lwdjd/IPFAR/internal/log"
)

// GraphQLBlockChecker 通过 Arweave GraphQL API 检查区块是否包含 IPFAR 数据
//
// 使用 SDK 的 GraphQL 查询构建器，按 Protocol Tag 搜索 IPFAR 元数据交易。
// 支持多网关自动故障转移（通过 SDK 的 MultiGatewayClient）。
type GraphQLBlockChecker struct {
	client  *sdkArweave.GatewayClient
	timeout time.Duration
}

// GraphQLCheckerConfig GraphQL 区块检查器配置
type GraphQLCheckerConfig struct {
	// GatewayURL Arweave 网关 URL（默认 https://arweave.net）
	GatewayURL string
	// Timeout GraphQL 查询超时（默认 30s）
	Timeout time.Duration
}

// DefaultGraphQLCheckerConfig 返回默认配置
func DefaultGraphQLCheckerConfig() GraphQLCheckerConfig {
	return GraphQLCheckerConfig{
		GatewayURL: "https://arweave.net",
		Timeout:    30 * time.Second,
	}
}

// NewGraphQLBlockChecker 创建 GraphQL 区块检查器
func NewGraphQLBlockChecker(cfg GraphQLCheckerConfig) *GraphQLBlockChecker {
	if cfg.GatewayURL == "" {
		cfg.GatewayURL = "https://arweave.net"
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}

	return &GraphQLBlockChecker{
		client:  sdkArweave.NewGatewayClient(cfg.GatewayURL),
		timeout: cfg.Timeout,
	}
}

// NewGraphQLBlockCheckerWithClient 使用已有的 SDK GatewayClient 创建检查器
func NewGraphQLBlockCheckerWithClient(client *sdkArweave.GatewayClient, timeout time.Duration) *GraphQLBlockChecker {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &GraphQLBlockChecker{
		client:  client,
		timeout: timeout,
	}
}

// CheckBlock 实现 BlockChecker 接口
//
// 通过 GraphQL 查询指定区块高度中标记为 IPFAR 协议的元数据交易。
// 查询标签：Protocol = "IPFS-Arweave-Bridge" AND IPFAR-Type = "meta"
// 使用 block height filter 限定到指定区块。
func (c *GraphQLBlockChecker) CheckBlock(height uint64) (found bool, metadataTXIDs []string, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// 构建 GraphQL 查询
	// 匹配标签：Protocol = "IPFS-Arweave-Bridge" 且 IPFAR-Type = "meta"
	// 同时也在 Base64URL 编码形式中匹配（兼容 goar chunked-upload tags）
	protocolB64 := base64.RawURLEncoding.EncodeToString([]byte("IPFS-Arweave-Bridge"))
	ipfarTypeB64 := base64.RawURLEncoding.EncodeToString([]byte("meta"))

	q := sdkArweave.NewGraphQLQuery().
		AddTagFilter("Protocol", "IPFS-Arweave-Bridge", protocolB64).
		AddTagFilter("IPFAR-Type", "meta", ipfarTypeB64).
		SetBlockRange(int(height), int(height)).
		SetFirst(100). // 同一区块中可能有多个 IPFAR 元数据交易
		SetSort("HEIGHT_ASC")

	txIDs, err := c.client.RunGraphQL(ctx, q)
	if err != nil {
		return false, nil, fmt.Errorf("GraphQL 查询失败 (height=%d): %w", height, err)
	}

	if len(txIDs) == 0 {
		return false, nil, nil
	}

	log.Debug("GraphQL 发现：区块 %d 包含 %d 个 IPFAR 元数据交易", height, len(txIDs))
	return true, txIDs, nil
}

// SetGatewayClient 更换网关客户端
func (c *GraphQLBlockChecker) SetGatewayClient(client *sdkArweave.GatewayClient) {
	c.client = client
}
