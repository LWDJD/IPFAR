// Package verify 桥节点 CAR v2 Index 验证封装
// 将内部调用委托给 SDK verify/ipfs 包
package verify

import (
	"fmt"

	"github.com/LWDJD/ipfar-sdk/verify/ipfs"
)

// ValidateIndex 验证 CAR v2 文件的 Index 段完整性
// 委托给 SDK 的 ipfs 包进行：
//  1. 解析 CAR 文件信息
//  2. 检查 Index 是否存在（规范 §3.4：无 Index 直接拒绝）
//  3. 验证 Index 内容完整性
//  4. 交叉验证 Index 与数据段
func ValidateIndex(carPath string) error {
	if carPath == "" {
		return fmt.Errorf("CAR file path is empty")
	}

	parser, err := ipfs.NewCarParserFromFile(carPath)
	if err != nil {
		return fmt.Errorf("无法打开 CAR 文件: %w", err)
	}
	defer parser.Close()

	info, err := parser.ParseInfo()
	if err != nil {
		return fmt.Errorf("无法解析 CAR 文件信息: %w", err)
	}

	// 仅 CARv2 需要索引
	if info.Version != 2 {
		return nil // CARv1 不检查索引
	}

	// 规范 §3.4：无 Index 直接拒绝
	if !info.HasIndex {
		return ipfs.ErrIndexNotFound
	}

	// 验证索引内容完整性
	if err := parser.ValidateIndexContent(); err != nil {
		return fmt.Errorf("索引内容验证失败: %w", err)
	}

	// 交叉验证索引与数据段
	if err := parser.ValidateIndexCrossCheck(); err != nil {
		return fmt.Errorf("索引交叉验证失败: %w", err)
	}

	return nil
}

// HasValidIndex 检查 CAR 文件是否有有效的 Index 段
// 返回 true 表示 Index 存在且格式正确
func HasValidIndex(carPath string) (bool, error) {
	parser, err := ipfs.NewCarParserFromFile(carPath)
	if err != nil {
		return false, fmt.Errorf("无法打开 CAR 文件: %w", err)
	}
	defer parser.Close()

	info, err := parser.ParseInfo()
	if err != nil {
		return false, fmt.Errorf("无法解析 CAR 文件信息: %w", err)
	}

	if info.Version != 2 {
		return false, nil
	}

	return info.HasIndex, nil
}

// GetIndexEntryCount 获取 CAR v2 Index 中的条目数量
func GetIndexEntryCount(carPath string) (int, error) {
	parser, err := ipfs.NewCarParserFromFile(carPath)
	if err != nil {
		return 0, fmt.Errorf("无法打开 CAR 文件: %w", err)
	}
	defer parser.Close()

	entries, err := parser.ParseIndex()
	if err != nil {
		return 0, fmt.Errorf("无法解析索引: %w", err)
	}

	return len(entries), nil
}
