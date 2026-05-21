// Package verify 桥节点 PoW 验证封装
// 将内部调用委托给 SDK pow.Verify()
package verify

import (
	"github.com/LWDJD/ipfar-sdk/verify/pow"
)

// VerifyPoW 验证工作量证明是否满足难度要求
// 委托给 SDK 的 pow.Verify()
// 规范参考: ipfar-specs/V1/项目规划.md §2.1
func VerifyPoW(powStr, powAlg, rootCID, dataTXID string, dataSize int64) error {
	return pow.Verify(powStr, powAlg, rootCID, dataTXID, dataSize)
}

// NeedsPoW 判断给定大小的文件是否需要 PoW 验证
// 规则：< 100 MiB 需要 PoW，≥ 100 MiB 免 PoW
// 委托给 SDK 的 pow.NeedsPoW()
func NeedsPoW(dataSize int64) bool {
	return pow.NeedsPoW(dataSize)
}
