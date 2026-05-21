// Package verify 桥节点 Arweave Tags 验证封装
// 将内部调用委托给 SDK metadata.ValidateTags()
package verify

import (
	sdkmeta "github.com/LWDJD/ipfar-sdk/verify/metadata"
)

// ValidateTags 验证 Arweave Transaction Tags 是否符合 IPFAR 规范
// 委托给 SDK 的 metadata.ValidateTags()
// 规范参考: ipfar-specs/V1/数据结构规范.md §1
func ValidateTags(tags []sdkmeta.Tag) error {
	return sdkmeta.ValidateTags(tags)
}

// BuildMetaTags 构建元数据交易所需的完整 Tags
// 委托给 SDK 的 metadata.BuildMetaTags()
func BuildMetaTags(rootCID, dataTXID string) []sdkmeta.Tag {
	return sdkmeta.BuildMetaTags(rootCID, dataTXID)
}

// BuildCARTags 构建 CAR 文件交易所需的完整 Tags
// 委托给 SDK 的 metadata.BuildCARTags()
func BuildCARTags(rootCID string, dataSize int64) []sdkmeta.Tag {
	return sdkmeta.BuildCARTags(rootCID, dataSize)
}
