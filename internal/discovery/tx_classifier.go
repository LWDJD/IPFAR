// Package discovery 提供 IPFAR 数据发现功能
//
// 交易分类器：判断 GraphQL 返回的交易是直接元数据、Bundle 包还是 CAR 文件。
// 用于区分 L1 标签可直接识别的交易和需要解析 Bundle 内部 DataItem 的交易。

package discovery

import (
	sdkArweave "github.com/LWDJD/ipfar-sdk/arweave"
	sdkBundle "github.com/LWDJD/ipfar-sdk/bundle"
)

// ClassifyByTags 根据 L1 交易的标签（已解码）分类交易类型。
//
// 返回值:
//   - "meta": 直接元数据交易（有 IPFAR-Type: meta 或 Content-Type: application/json）
//   - "car": 直接 CAR 文件交易（Content-Type: application/vnd.ipld.car）
//   - "bundle": 可能是 Bundle 包（有 Protocol: IPFS-Arweave-Bridge 但无 IPFAR-Type: meta）
//   - "unknown": 无法确定类型
func ClassifyByTags(tags []sdkArweave.Tag) string {
	hasProtocol := false
	hasIPFARTypeMeta := false
	hasContentTypeJSON := false
	hasContentTypeCAR := false

	for _, tag := range tags {
		switch tag.Name {
		case "Protocol":
			if tag.Value == "IPFS-Arweave-Bridge" {
				hasProtocol = true
			}
		case "IPFAR-Type":
			if tag.Value == "meta" {
				hasIPFARTypeMeta = true
			}
		case "Content-Type":
			if tag.Value == "application/json" {
				hasContentTypeJSON = true
			}
			if tag.Value == "application/vnd.ipld.car" {
				hasContentTypeCAR = true
			}
		}
	}

	if !hasProtocol {
		return "unknown"
	}

	if hasContentTypeCAR {
		return "car"
	}

	if hasIPFARTypeMeta || hasContentTypeJSON {
		return "meta"
	}

	// 有 Protocol 但无 IPFAR-Type 和明确的 Content-Type → 可能是 Bundle
	return "bundle"
}

// ClassifyByData 根据交易数据内容分类交易类型。
//
// 相比 ClassifyByTags，此方法不需要额外的 HTTP 请求获取标签，
// 直接通过数据内容判断：
//   - JSON 开头（'{'） → "meta"（直接元数据 JSON）
//   - 可成功解析为 ANS-104 Bundle → "bundle"
//   - 其他 → "unknown"
func ClassifyByData(data []byte) string {
	if len(data) == 0 {
		return "unknown"
	}

	// JSON 元数据以 '{' 开头
	if data[0] == '{' {
		return "meta"
	}

	// 尝试解析为 ANS-104 Bundle
	if _, err := sdkBundle.ParseBundle(data); err == nil {
		return "bundle"
	}

	return "unknown"
}

// HasMetaTag 检查 DataItem 的 tags 中是否包含 IPFAR-Type: meta
func HasMetaTag(tags []sdkBundle.Tag) bool {
	for _, tag := range tags {
		if tag.Name == "IPFAR-Type" && tag.Value == "meta" {
			return true
		}
	}
	return false
}
