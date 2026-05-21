// Package verify 桥节点元数据验证封装
// 将内部调用委托给 SDK metadata.Validate()
package verify

import (
	sdkmeta "github.com/LWDJD/ipfar-sdk/verify/metadata"
)

// ValidateMetadata 验证元数据的完整性和合法性
// 委托给 SDK 的 metadata.Validate()
// 规范参考: ipfar-specs/V1/数据结构规范.md §2
func ValidateMetadata(meta *sdkmeta.Metadata) error {
	if meta == nil {
		return sdkmeta.ErrInvalidJSON
	}
	return meta.Validate()
}

// ParseAndValidateMetadata 解析 JSON 字节为元数据并验证
// 委托给 SDK 的 metadata.ParseAndValidate()
func ParseAndValidateMetadata(data []byte) (*sdkmeta.Metadata, error) {
	return sdkmeta.ParseAndValidate(data)
}

// ParseAndValidateMetadataBase64URL 从 Base64URL 字符串解析并验证元数据
// 委托给 SDK 的 metadata.ParseAndValidateBase64URL()
func ParseAndValidateMetadataBase64URL(encoded string) (*sdkmeta.Metadata, error) {
	return sdkmeta.ParseAndValidateBase64URL(encoded)
}
