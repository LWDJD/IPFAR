// Package discovery 提供 IPFAR 数据发现功能
//
// Bitlist 结构：[]uint64 bitset，标记块高是否存在 IPFS 数据
// 规范参考: ipfar-specs/V1/项目规划.md §3.1
//
// 每一位表示一个块高度，0 表示无 IPFS 数据，1 表示有。
// 配合不可信分发机制使用——bitlist 极小，可以轻易存储几十上百个不可信导航。
package discovery

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
)

// Bitlist 错误定义
var (
	ErrInvalidBitlist    = errors.New("invalid bitlist data")
	ErrInvalidBitlistLen = errors.New("invalid bitlist length")
	ErrEmptyBitlist      = errors.New("bitlist is empty")
)

// Bitlist 块高度位图
// 每 64 位一组 (uint64)，位索引 i 对应块高度 i
type Bitlist []uint64

// NewBitlist 创建指定容量（块高度范围）的 Bitlist
// maxHeight: 最大块高度（含）
func NewBitlist(maxHeight uint64) Bitlist {
	if maxHeight == 0 {
		return Bitlist{}
	}
	// 需要 ceil((maxHeight+1) / 64) 个 uint64
	size := (maxHeight + 64) / 64
	return make(Bitlist, size)
}

// NewBitlistFromBytes 从字节数组解码 Bitlist
// 采用 BigEndian uint64 编码，每 8 字节一组
func NewBitlistFromBytes(data []byte) (Bitlist, error) {
	if len(data) == 0 {
		return nil, ErrEmptyBitlist
	}
	if len(data)%8 != 0 {
		return nil, fmt.Errorf("%w: length %d not multiple of 8", ErrInvalidBitlistLen, len(data))
	}

	size := len(data) / 8
	bl := make(Bitlist, size)
	for i := 0; i < size; i++ {
		bl[i] = binary.BigEndian.Uint64(data[i*8 : (i+1)*8])
	}
	return bl, nil
}

// ToBytes 将 Bitlist 编码为字节数组（BigEndian uint64）
func (bl Bitlist) ToBytes() []byte {
	if len(bl) == 0 {
		return nil
	}
	data := make([]byte, len(bl)*8)
	for i, v := range bl {
		binary.BigEndian.PutUint64(data[i*8:(i+1)*8], v)
	}
	return data
}

// Set 设置块高度对应位为 1（标记为有数据）
func (bl Bitlist) Set(height uint64) error {
	idx := height / 64
	if int(idx) >= len(bl) {
		return fmt.Errorf("height %d out of range (max %d)", height, (uint64(len(bl))*64)-1)
	}
	bit := height % 64
	bl[idx] |= (1 << bit)
	return nil
}

// Unset 设置块高度对应位为 0（标记为无数据）
func (bl Bitlist) Unset(height uint64) error {
	idx := height / 64
	if int(idx) >= len(bl) {
		return fmt.Errorf("height %d out of range (max %d)", height, (uint64(len(bl))*64)-1)
	}
	bit := height % 64
	bl[idx] &^= (1 << bit)
	return nil
}

// Get 获取块高度对应位
// 如果高度超出范围，返回 false
func (bl Bitlist) Get(height uint64) bool {
	idx := height / 64
	if int(idx) >= len(bl) {
		return false
	}
	bit := height % 64
	return (bl[idx] & (1 << bit)) != 0
}

// Count 统计标记为 1 的位数量（PopCount）
func (bl Bitlist) Count() int {
	total := 0
	for _, v := range bl {
		total += bits.OnesCount64(v)
	}
	return total
}

// MaxHeight 返回 Bitlist 支持的最大块高度
func (bl Bitlist) MaxHeight() uint64 {
	if len(bl) == 0 {
		return 0
	}
	return uint64(len(bl)*64) - 1
}

// Size 返回 Bitlist 的 uint64 组数
func (bl Bitlist) Size() int {
	return len(bl)
}

// Or 按位或合并另一个 Bitlist（用于合并多个不可信导航）
// 两个 Bitlist 取其长者作为结果长度，短者高位补 0
func (bl Bitlist) Or(other Bitlist) Bitlist {
	maxLen := len(bl)
	if len(other) > maxLen {
		maxLen = len(other)
	}

	result := make(Bitlist, maxLen)
	for i := 0; i < len(bl); i++ {
		result[i] = bl[i]
	}
	for i := 0; i < len(other); i++ {
		result[i] |= other[i]
	}

	return result
}

// And 按位与合并另一个 Bitlist（取交集）
func (bl Bitlist) And(other Bitlist) Bitlist {
	maxLen := len(bl)
	if len(other) > maxLen {
		maxLen = len(other)
	}

	result := make(Bitlist, maxLen)
	for i := 0; i < len(bl) && i < len(other); i++ {
		result[i] = bl[i] & other[i]
	}

	return result
}

// Xor 按位异或（用于比较差异）
func (bl Bitlist) Xor(other Bitlist) Bitlist {
	maxLen := len(bl)
	if len(other) > maxLen {
		maxLen = len(other)
	}

	result := make(Bitlist, maxLen)
	for i := 0; i < maxLen; i++ {
		if i < len(bl) {
			result[i] = bl[i]
		}
		if i < len(other) {
			result[i] ^= other[i]
		}
	}

	return result
}

// Clone 深拷贝
func (bl Bitlist) Clone() Bitlist {
	result := make(Bitlist, len(bl))
	copy(result, bl)
	return result
}

// SetRange 批量设置 [startHeight, endHeight] 范围内的所有位为 1
func (bl Bitlist) SetRange(startHeight, endHeight uint64) error {
	maxH := bl.MaxHeight()
	if endHeight > maxH {
		return fmt.Errorf("endHeight %d out of range (max %d)", endHeight, maxH)
	}
	if startHeight > endHeight {
		return fmt.Errorf("startHeight %d > endHeight %d", startHeight, endHeight)
	}

	for h := startHeight; h <= endHeight; h++ {
		// 内联 set 避免错误检查开销
		idx := h / 64
		bit := h % 64
		bl[idx] |= (1 << bit)
	}
	return nil
}

// Ones 返回所有标记为 1 的块高度列表
func (bl Bitlist) Ones() []uint64 {
	result := make([]uint64, 0, bl.Count())
	for i, v := range bl {
		if v == 0 {
			continue
		}
		base := uint64(i) * 64
		for bit := 0; bit < 64; bit++ {
			if v&(1<<bit) != 0 {
				result = append(result, base+uint64(bit))
			}
		}
	}
	return result
}

// Zeros 返回 [0, maxHeight] 范围内所有标记为 0 的块高度列表
func (bl Bitlist) Zeros() []uint64 {
	if len(bl) == 0 {
		return nil
	}
	maxH := bl.MaxHeight()
	result := make([]uint64, 0, int(maxH+1)-bl.Count())
	for h := uint64(0); h <= maxH; h++ {
		if !bl.Get(h) {
			result = append(result, h)
		}
	}
	return result
}

// Equal 判断两个 Bitlist 是否相等
func (bl Bitlist) Equal(other Bitlist) bool {
	if len(bl) != len(other) {
		return false
	}
	for i := range bl {
		if bl[i] != other[i] {
			return false
		}
	}
	return true
}

// IsEmpty 判断是否全为 0（无标记）
func (bl Bitlist) IsEmpty() bool {
	for _, v := range bl {
		if v != 0 {
			return false
		}
	}
	return true
}

// Extend 扩展 Bitlist 到新的 maxHeight
// 如果新的 maxHeight 小于当前，则截断
func (bl Bitlist) Extend(newMaxHeight uint64) Bitlist {
	newSize := int((newMaxHeight + 64) / 64)
	if newSize <= len(bl) {
		return bl[:newSize]
	}
	result := make(Bitlist, newSize)
	copy(result, bl)
	return result
}

// Merge 合并多个 Bitlist（OR 语义）
func Merge(bitlists ...Bitlist) Bitlist {
	if len(bitlists) == 0 {
		return nil
	}
	result := bitlists[0].Clone()
	for i := 1; i < len(bitlists); i++ {
		result = result.Or(bitlists[i])
	}
	return result
}

// Intersect 取多个 Bitlist 的交集（AND 语义）
func Intersect(bitlists ...Bitlist) Bitlist {
	if len(bitlists) == 0 {
		return nil
	}
	result := bitlists[0].Clone()
	for i := 1; i < len(bitlists); i++ {
		result = result.And(bitlists[i])
	}
	return result
}

// FromHeights 从高度列表创建 Bitlist
func FromHeights(maxHeight uint64, heights ...uint64) (Bitlist, error) {
	bl := NewBitlist(maxHeight)
	for _, h := range heights {
		if err := bl.Set(h); err != nil {
			return nil, err
		}
	}
	return bl, nil
}

// Compact 压缩 Bitlist（去除末尾全零的 uint64 组）
func (bl Bitlist) Compact() Bitlist {
	lastNonZero := len(bl) - 1
	for lastNonZero >= 0 && bl[lastNonZero] == 0 {
		lastNonZero--
	}
	if lastNonZero < 0 {
		return Bitlist{}
	}
	return bl[:lastNonZero+1]
}
