// Package discovery 提供 IPFAR 数据发现功能
// 规范参考: ipfar-specs/V1/项目规划.md §3.1
//
// 通过去重随机抽样定位 IPFS 数据所在的区块，使用可选的 bitlist 引导标记块高是否存在 IPFS 数据，
// 配合不可信分发机制实现去中心化发现。
package discovery

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/lwdjd/IPFAR/internal/log"
)

// BlockChecker 区块检查器接口
// 实现此接口以提供具体的区块查询逻辑（例如通过 Arweave 网关 API）
type BlockChecker interface {
	// CheckBlock 检查指定高度区块是否包含 IPFAR 数据
	// 返回 (found bool, metadataTXIDs []string, err error)
	CheckBlock(height uint64) (found bool, metadataTXIDs []string, err error)
}

// SamplerConfig 采样器配置
type SamplerConfig struct {
	// MinHeight 抽样范围下限（含）
	MinHeight uint64
	// MaxHeight 抽样范围上限（含）
	MaxHeight uint64
	// PollInterval 定时轮询最新块的间隔
	// 为 0 时不启用轮询
	PollInterval time.Duration
	// MaxChecked 最大已检查块数量（内存限制 1.6G）
	// 超过此数量后，最旧的记录会被清除
	MaxChecked int
}

// DefaultSamplerConfig 返回默认配置
func DefaultSamplerConfig() SamplerConfig {
	return SamplerConfig{
		MinHeight:    0,
		MaxHeight:    0, // 0 表示动态跟随最新块
		PollInterval: 2 * time.Minute,
		MaxChecked:   1_000_000, // 100 万条记录约 < 50 MB
	}
}

// Sampler 去重随机抽样器
// 线程安全
type Sampler struct {
	mu sync.RWMutex

	config SamplerConfig
	checker BlockChecker

	// checked 已检查的块高度集合（去重）
	checked map[uint64]bool
	// checkOrder 按检查顺序记录高度（用于 LRU 淘汰）
	checkOrder []uint64

	// found 已发现含 IPFAR 数据的块
	found map[uint64][]string // height → metadata TXIDs

	// stats 统计信息
	stats SamplerStats

	// stopCh 停止轮询信号
	stopCh chan struct{}
	// stopped 是否已停止
	stopped bool
}

// SamplerStats 采样器统计信息
type SamplerStats struct {
	TotalChecked  uint64 `json:"total_checked"`  // 总检查次数
	TotalFound    uint64 `json:"total_found"`    // 总发现次数
	TotalMissed   uint64 `json:"total_missed"`   // 总未发现次数
	TotalErrors   uint64 `json:"total_errors"`   // 总错误次数
	LastCheckedAt int64  `json:"last_checked_at"` // 上次检查时间（Unix 纳秒）
	LastHeight    uint64 `json:"last_height"`    // 上次检查的块高度
}

// NewSampler 创建新的采样器
func NewSampler(config SamplerConfig, checker BlockChecker) *Sampler {
	if config.MaxChecked <= 0 {
		config.MaxChecked = 1_000_000
	}

	return &Sampler{
		config:     config,
		checker:    checker,
		checked:    make(map[uint64]bool),
		checkOrder: make([]uint64, 0, config.MaxChecked),
		found:      make(map[uint64][]string),
		stopCh:     make(chan struct{}),
	}
}

// StartPolling 启动定时轮询最新区块
func (s *Sampler) StartPolling() {
	if s.config.PollInterval <= 0 {
		return
	}

	go func() {
		ticker := time.NewTicker(s.config.PollInterval)
		defer ticker.Stop()

		log.Info("发现采样器：启动定时轮询，间隔=%v", s.config.PollInterval)

		for {
			select {
			case <-ticker.C:
				s.pollLatestBlock()
			case <-s.stopCh:
				log.Info("发现采样器：定时轮询已停止")
				return
			}
		}
	}()
}

// Stop 停止采样器（包括定时轮询）
func (s *Sampler) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.stopped {
		s.stopped = true
		close(s.stopCh)
	}
}

// Sample 执行一次去重随机抽样
// 在 [minHeight, maxHeight] 范围内随机选取一个未检查过的块高度，
// 检查该块是否包含 IPFAR 数据
func (s *Sampler) Sample() (*SampleResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	minH, maxH := s.effectiveRange()

	// 计算未检查的块数量
	rangeSize := maxH - minH + 1
	checkedCount := uint64(len(s.checked))

	if checkedCount >= rangeSize {
		return &SampleResult{
			Exhausted: true,
			Message:   fmt.Sprintf("范围内所有 %d 个块已检查完毕", rangeSize),
		}, nil
	}

	// 随机选择一个块高度
	height, err := s.randomUncheckedHeight(minH, maxH)
	if err != nil {
		return nil, fmt.Errorf("随机选取块高度失败: %w", err)
	}

	// 检查该块
	result, err := s.checkBlock(height)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// SampleN 执行 N 次去重随机抽样
func (s *Sampler) SampleN(n int) ([]*SampleResult, error) {
	if n <= 0 {
		return nil, fmt.Errorf("n must be positive, got %d", n)
	}

	results := make([]*SampleResult, 0, n)
	for i := 0; i < n; i++ {
		result, err := s.Sample()
		if err != nil {
			return results, fmt.Errorf("第 %d 次抽样失败: %w", i+1, err)
		}
		results = append(results, result)

		// 如果已耗尽，停止
		if result.Exhausted {
			break
		}
	}
	return results, nil
}

// SampleFromRange 在指定范围内执行一次去重随机抽样
func (s *Sampler) SampleFromRange(minHeight, maxHeight uint64) (*SampleResult, error) {
	s.mu.Lock()
	originalMin, originalMax := s.config.MinHeight, s.config.MaxHeight
	s.config.MinHeight = minHeight
	s.config.MaxHeight = maxHeight
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.config.MinHeight = originalMin
		s.config.MaxHeight = originalMax
		s.mu.Unlock()
	}()

	return s.Sample()
}

// pollLatestBlock 轮询最新区块
func (s *Sampler) pollLatestBlock() {
	s.mu.Lock()
	maxH := s.config.MaxHeight
	s.mu.Unlock()

	// 如果 MaxHeight 为 0，需要外部动态设置
	if maxH == 0 {
		log.Debug("发现采样器：MaxHeight=0，跳过轮询（需要外部更新范围）")
		return
	}

	// 将最新区块加入待检查范围并抽样
	result, err := s.SampleFromRange(maxH, maxH)
	if err != nil {
		log.Warn("发现采样器：轮询最新块 %d 失败: %v", maxH, err)
		return
	}

	if result.Found {
		log.Info("发现采样器：轮询发现数据！高度=%d, 元数据数量=%d", result.Height, len(result.MetadataTXIDs))
	}
}

// checkBlock 检查指定块高度（内部方法，调用前需持有锁）
func (s *Sampler) checkBlock(height uint64) (*SampleResult, error) {
	// 记录为已检查
	s.markChecked(height)

	result := &SampleResult{
		Height:    height,
		CheckedAt: time.Now(),
	}

	// 调用外部检查器
	found, txIDs, err := s.checker.CheckBlock(height)
	if err != nil {
		s.stats.TotalErrors++
		result.Error = err.Error()
		log.Debug("发现采样器：检查块 %d 出错: %v", height, err)
		return result, nil
	}

	s.stats.LastCheckedAt = time.Now().UnixNano()
	s.stats.LastHeight = height
	s.stats.TotalChecked++

	if found {
		s.stats.TotalFound++
		s.found[height] = txIDs
		result.Found = true
		result.MetadataTXIDs = txIDs
		log.Info("发现采样器：块 %d 包含 IPFAR 数据，元数据交易数=%d", height, len(txIDs))
	} else {
		s.stats.TotalMissed++
		result.Found = false
	}

	return result, nil
}

// markChecked 标记块高度为已检查（内部方法，调用前需持有锁）
func (s *Sampler) markChecked(height uint64) {
	if s.checked[height] {
		return
	}

	// LRU 淘汰：超过最大数量时删除最旧的记录
	if len(s.checkOrder) >= s.config.MaxChecked {
		oldest := s.checkOrder[0]
		s.checkOrder = s.checkOrder[1:]
		delete(s.checked, oldest)
		delete(s.found, oldest)
	}

	s.checked[height] = true
	s.checkOrder = append(s.checkOrder, height)
}

// randomUncheckedHeight 随机选取一个未检查的块高度（内部方法，调用前需持有锁）
func (s *Sampler) randomUncheckedHeight(minH, maxH uint64) (uint64, error) {
	rangeSize := maxH - minH + 1

	// 如果范围小，直接扫描找到未检查的
	if rangeSize < 1000 {
		unchecked := make([]uint64, 0, rangeSize)
		for h := minH; h <= maxH; h++ {
			if !s.checked[h] {
				unchecked = append(unchecked, h)
			}
		}
		if len(unchecked) == 0 {
			return 0, fmt.Errorf("no unchecked blocks in range [%d, %d]", minH, maxH)
		}
		idx, err := cryptoRandInt(len(unchecked))
		if err != nil {
			return 0, err
		}
		return unchecked[idx], nil
	}

	// 大范围：随机尝试直到找到未检查的（最多尝试 100 次）
	for attempt := 0; attempt < 100; attempt++ {
		offset, err := cryptoRandInt(int(rangeSize))
		if err != nil {
			return 0, err
		}
		height := minH + uint64(offset)
		if !s.checked[height] {
			return height, nil
		}
	}

	// 随机尝试失败，回退到顺序扫描
	for h := minH; h <= maxH; h++ {
		if !s.checked[h] {
			return h, nil
		}
	}

	return 0, fmt.Errorf("no unchecked blocks in range [%d, %d]", minH, maxH)
}

// effectiveRange 返回有效的抽样范围（内部方法，调用前需持有锁）
func (s *Sampler) effectiveRange() (uint64, uint64) {
	minH := s.config.MinHeight
	maxH := s.config.MaxHeight
	if maxH == 0 {
		maxH = s.stats.LastHeight
	}
	if maxH < minH {
		maxH = minH
	}
	return minH, maxH
}

// cryptoRandInt 加密安全随机整数 [0, max)
func cryptoRandInt(max int) (int, error) {
	if max <= 0 {
		return 0, nil
	}
	n, err := rand.Int(rand.Reader, big.NewInt(int64(max)))
	if err != nil {
		return 0, err
	}
	return int(n.Int64()), nil
}

// ============================================================
// 查询方法
// ============================================================

// IsChecked 检查块高度是否已被检查
func (s *Sampler) IsChecked(height uint64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.checked[height]
}

// FoundAt 获取在指定高度发现的元数据交易 ID 列表
func (s *Sampler) FoundAt(height uint64) ([]string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	txIDs, ok := s.found[height]
	return txIDs, ok
}

// GetAllFound 获取所有已发现的块高度及其元数据交易
func (s *Sampler) GetAllFound() map[uint64][]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[uint64][]string, len(s.found))
	for k, v := range s.found {
		result[k] = v
	}
	return result
}

// Stats 获取统计信息
func (s *Sampler) Stats() SamplerStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.stats
}

// CheckedCount 返回已检查的块数量
func (s *Sampler) CheckedCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.checked)
}

// UpdateMaxHeight 更新最大块高度（用于动态跟随链增长）
func (s *Sampler) UpdateMaxHeight(height uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if height > s.config.MaxHeight {
		s.config.MaxHeight = height
	}
}

// UpdateRange 更新抽样范围
func (s *Sampler) UpdateRange(minHeight, maxHeight uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.config.MinHeight = minHeight
	s.config.MaxHeight = maxHeight
}

// ============================================================
// 类型定义
// ============================================================

// SampleResult 单次抽样结果
type SampleResult struct {
	Height        uint64    `json:"height"`          // 抽样的块高度
	Found         bool      `json:"found"`           // 是否发现 IPFAR 数据
	MetadataTXIDs []string  `json:"metadata_txids,omitempty"` // 发现的元数据交易 ID 列表
	CheckedAt     time.Time `json:"checked_at"`       // 检查时间
	Error         string    `json:"error,omitempty"`  // 错误信息
	Exhausted     bool      `json:"exhausted,omitempty"` // 是否已耗尽
	Message       string    `json:"message,omitempty"` // 附加消息
}

// cryptoRandUint64 加密安全随机 uint64
func cryptoRandUint64() (uint64, error) {
	b := make([]byte, 8)
	_, err := rand.Read(b)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(b), nil
}
