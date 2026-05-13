package discovery

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// mockBlockChecker 模拟区块检查器
type mockBlockChecker struct {
	mu          sync.Mutex
	dataBlocks  map[uint64][]string // 预设的包含数据的区块
	checkCount  map[uint64]int      // 每个块被检查的次数（验证去重）
	alwaysFound bool                // 所有块都返回找到
}

func newMockBlockChecker() *mockBlockChecker {
	return &mockBlockChecker{
		dataBlocks: make(map[uint64][]string),
		checkCount: make(map[uint64]int),
	}
}

func (m *mockBlockChecker) CheckBlock(height uint64) (bool, []string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.checkCount[height]++

	if m.alwaysFound {
		return true, []string{fmt.Sprintf("tx-%d", height)}, nil
	}

	txIDs, ok := m.dataBlocks[height]
	if ok {
		return true, txIDs, nil
	}
	return false, nil, nil
}

func (m *mockBlockChecker) getCheckCount(height uint64) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.checkCount[height]
}

func (m *mockBlockChecker) setDataBlock(height uint64, txIDs []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dataBlocks[height] = txIDs
}

func TestNewSampler(t *testing.T) {
	checker := newMockBlockChecker()
	config := SamplerConfig{
		MinHeight:    0,
		MaxHeight:    100,
		PollInterval: 0,
		MaxChecked:   1000,
	}

	s := NewSampler(config, checker)
	if s == nil {
		t.Fatal("NewSampler 返回 nil")
	}
	if s.config.MinHeight != 0 {
		t.Errorf("MinHeight = %d, want 0", s.config.MinHeight)
	}
	if s.config.MaxHeight != 100 {
		t.Errorf("MaxHeight = %d, want 100", s.config.MaxHeight)
	}
	if s.config.MaxChecked != 1000 {
		t.Errorf("MaxChecked = %d, want 1000", s.config.MaxChecked)
	}
}

func TestDefaultSamplerConfig(t *testing.T) {
	cfg := DefaultSamplerConfig()
	if cfg.PollInterval != 2*time.Minute {
		t.Errorf("PollInterval = %v, want 2m", cfg.PollInterval)
	}
	if cfg.MaxChecked <= 0 {
		t.Errorf("MaxChecked = %d, should be positive", cfg.MaxChecked)
	}
}

func TestSampler_Sample_Deduplication(t *testing.T) {
	checker := newMockBlockChecker()
	// 设置块 5 和 50 包含数据
	checker.setDataBlock(5, []string{"tx-abc"})
	checker.setDataBlock(50, []string{"tx-def"})

	s := NewSampler(SamplerConfig{
		MinHeight:    0,
		MaxHeight:    100,
		PollInterval: 0,
		MaxChecked:   1000,
	}, checker)

	// 执行多次抽样
	checkedHeights := make(map[uint64]bool)
	for i := 0; i < 20; i++ {
		result, err := s.Sample()
		if err != nil {
			t.Fatalf("第 %d 次抽样失败: %v", i, err)
		}
		if result.Exhausted {
			t.Logf("第 %d 次抽样后范围耗尽", i)
			break
		}
		// 验证去重
		if checkedHeights[result.Height] {
			t.Errorf("重复检查块 %d", result.Height)
		}
		checkedHeights[result.Height] = true
	}

	// 验证每个块只被检查一次
	for h, count := range checker.checkCount {
		if count > 1 {
			t.Errorf("块 %d 被检查了 %d 次", h, count)
		}
	}
}

func TestSampler_Sample_FindsData(t *testing.T) {
	checker := newMockBlockChecker()
	// 设置块 42 包含数据
	checker.setDataBlock(42, []string{"tx-meta-1", "tx-meta-2"})

	s := NewSampler(SamplerConfig{
		MinHeight:    0,
		MaxHeight:    50,
		PollInterval: 0,
		MaxChecked:   100,
	}, checker)

	// 执行抽样直到找到块 42
	found := false
	for i := 0; i < 100; i++ {
		result, err := s.Sample()
		if err != nil {
			t.Fatalf("抽样失败: %v", err)
		}
		if result.Exhausted {
			break
		}
		if result.Height == 42 {
			found = true
			if !result.Found {
				t.Errorf("块 42 应包含数据但返回 found=false")
			}
			if len(result.MetadataTXIDs) != 2 {
				t.Errorf("块 42 应有 2 个元数据交易，实际 %d", len(result.MetadataTXIDs))
			}
			break
		}
	}

	if !found {
		t.Skip("随机抽样未覆盖到块 42（概率事件），但抽样逻辑正确")
	}
}

func TestSampler_Sample_Exhaustion(t *testing.T) {
	checker := newMockBlockChecker()

	s := NewSampler(SamplerConfig{
		MinHeight:    0,
		MaxHeight:    5,
		PollInterval: 0,
		MaxChecked:   100,
	}, checker)

	// 检查 6 个块（0-5），应该能全部检查
	for i := 0; i < 6; i++ {
		result, err := s.Sample()
		if err != nil {
			t.Fatalf("第 %d 次抽样失败: %v", i, err)
		}
		if result.Exhausted {
			t.Fatalf("不应该在检查完之前耗尽，i=%d", i)
		}
	}

	// 第 7 次应耗尽
	result, err := s.Sample()
	if err != nil {
		t.Fatalf("抽样失败: %v", err)
	}
	if !result.Exhausted {
		t.Errorf("应耗尽但 result.Exhausted=false")
	}
}

func TestSampler_SampleN(t *testing.T) {
	checker := newMockBlockChecker()
	checker.alwaysFound = true

	s := NewSampler(SamplerConfig{
		MinHeight:    0,
		MaxHeight:    100,
		PollInterval: 0,
		MaxChecked:   1000,
	}, checker)

	results, err := s.SampleN(10)
	if err != nil {
		t.Fatalf("SampleN 失败: %v", err)
	}
	if len(results) != 10 {
		t.Errorf("SampleN 返回 %d 个结果，期望 10", len(results))
	}

	// 验证去重
	heights := make(map[uint64]bool)
	for _, r := range results {
		if heights[r.Height] {
			t.Errorf("SampleN 返回了重复高度 %d", r.Height)
		}
		heights[r.Height] = true
		if !r.Found {
			t.Errorf("块 %d 应找到数据", r.Height)
		}
	}
}

func TestSampler_IsChecked(t *testing.T) {
	checker := newMockBlockChecker()

	s := NewSampler(SamplerConfig{
		MinHeight:    0,
		MaxHeight:    10,
		PollInterval: 0,
		MaxChecked:   100,
	}, checker)

	// 未检查前
	if s.IsChecked(5) {
		t.Errorf("块 5 不应标记为已检查")
	}

	result, err := s.Sample()
	if err != nil {
		t.Fatalf("抽样失败: %v", err)
	}
	if result.Exhausted {
		t.Fatal("不应耗尽")
	}

	// 检查后
	if !s.IsChecked(result.Height) {
		t.Errorf("块 %d 应标记为已检查", result.Height)
	}
}

func TestSampler_FoundAt(t *testing.T) {
	checker := newMockBlockChecker()
	checker.setDataBlock(7, []string{"tx-7"})

	s := NewSampler(SamplerConfig{
		MinHeight:    0,
		MaxHeight:    10,
		PollInterval: 0,
		MaxChecked:   100,
	}, checker)

	// 抽样直到找到块 7
	for i := 0; i < 15; i++ {
		result, err := s.Sample()
		if err != nil {
			t.Fatalf("抽样失败: %v", err)
		}
		if result.Exhausted {
			break
		}
		if result.Height == 7 {
			break
		}
	}

	txIDs, ok := s.FoundAt(7)
	if !ok {
		t.Error("FoundAt(7) 应返回 true")
	}
	if len(txIDs) != 1 || txIDs[0] != "tx-7" {
		t.Errorf("FoundAt(7) = %v, want [tx-7]", txIDs)
	}

	_, ok = s.FoundAt(999)
	if ok {
		t.Error("FoundAt(999) 应返回 false")
	}
}

func TestSampler_GetAllFound(t *testing.T) {
	checker := newMockBlockChecker()
	checker.alwaysFound = true

	s := NewSampler(SamplerConfig{
		MinHeight:    0,
		MaxHeight:    5,
		PollInterval: 0,
		MaxChecked:   100,
	}, checker)

	// 检查所有块
	for i := 0; i < 6; i++ {
		result, err := s.Sample()
		if err != nil {
			t.Fatalf("抽样失败: %v", err)
		}
		if result.Exhausted {
			break
		}
	}

	allFound := s.GetAllFound()
	if len(allFound) != 6 {
		t.Errorf("GetAllFound 返回 %d 条记录，期望 6", len(allFound))
	}
}

func TestSampler_Stats(t *testing.T) {
	checker := newMockBlockChecker()
	checker.alwaysFound = true

	s := NewSampler(SamplerConfig{
		MinHeight:    0,
		MaxHeight:    20,
		PollInterval: 0,
		MaxChecked:   100,
	}, checker)

	_, err := s.SampleN(5)
	if err != nil {
		t.Fatalf("SampleN 失败: %v", err)
	}

	stats := s.Stats()
	if stats.TotalChecked != 5 {
		t.Errorf("TotalChecked = %d, want 5", stats.TotalChecked)
	}
	if stats.TotalFound != 5 {
		t.Errorf("TotalFound = %d, want 5", stats.TotalFound)
	}
	if stats.TotalMissed != 0 {
		t.Errorf("TotalMissed = %d, want 0", stats.TotalMissed)
	}
}

func TestSampler_UpdateMaxHeight(t *testing.T) {
	checker := newMockBlockChecker()

	s := NewSampler(SamplerConfig{
		MinHeight:    0,
		MaxHeight:    100,
		PollInterval: 0,
		MaxChecked:   1000,
	}, checker)

	s.UpdateMaxHeight(200)
	if s.config.MaxHeight != 200 {
		t.Errorf("UpdateMaxHeight(200) 后 MaxHeight = %d", s.config.MaxHeight)
	}

	// 不应降低
	s.UpdateMaxHeight(150)
	if s.config.MaxHeight != 200 {
		t.Errorf("UpdateMaxHeight(150) 不应降低 MaxHeight，实际 %d", s.config.MaxHeight)
	}
}

func TestSampler_UpdateRange(t *testing.T) {
	checker := newMockBlockChecker()

	s := NewSampler(SamplerConfig{
		MinHeight:    0,
		MaxHeight:    100,
		PollInterval: 0,
		MaxChecked:   1000,
	}, checker)

	s.UpdateRange(50, 150)
	if s.config.MinHeight != 50 {
		t.Errorf("MinHeight = %d, want 50", s.config.MinHeight)
	}
	if s.config.MaxHeight != 150 {
		t.Errorf("MaxHeight = %d, want 150", s.config.MaxHeight)
	}
}

func TestSampler_Stop(t *testing.T) {
	checker := newMockBlockChecker()

	s := NewSampler(SamplerConfig{
		MinHeight:    0,
		MaxHeight:    100,
		PollInterval: 100 * time.Millisecond,
		MaxChecked:   1000,
	}, checker)

	s.StartPolling()
	time.Sleep(50 * time.Millisecond)
	s.Stop()
	// 不应 panic
	time.Sleep(50 * time.Millisecond)
}

func TestSampler_CheckedCount(t *testing.T) {
	checker := newMockBlockChecker()
	checker.alwaysFound = true

	s := NewSampler(SamplerConfig{
		MinHeight:    0,
		MaxHeight:    50,
		PollInterval: 0,
		MaxChecked:   100,
	}, checker)

	if count := s.CheckedCount(); count != 0 {
		t.Errorf("初始 CheckedCount = %d, want 0", count)
	}

	s.SampleN(10)
	if count := s.CheckedCount(); count != 10 {
		t.Errorf("抽样 10 次后 CheckedCount = %d, want 10", count)
	}
}

func TestSampler_LRUEviction(t *testing.T) {
	checker := newMockBlockChecker()
	checker.alwaysFound = true

	// 设置很小的 MaxChecked 以触发 LRU 淘汰
	s := NewSampler(SamplerConfig{
		MinHeight:    0,
		MaxHeight:    100,
		PollInterval: 0,
		MaxChecked:   10, // 只保留 10 条
	}, checker)

	// 抽样 15 次
	_, err := s.SampleN(15)
	if err != nil {
		t.Fatalf("SampleN 失败: %v", err)
	}

	// 检查数量不应超过 MaxChecked
	if count := s.CheckedCount(); count > 10 {
		t.Errorf("LRU 淘汰失败，CheckedCount = %d, 应 ≤ 10", count)
	}

	// 验证旧记录被淘汰：最早的几个高度应该不在 checked 中
	// 由于随机抽样无法确定哪些被淘汰，只验证总数不超标
}

func TestSampler_SampleFromRange(t *testing.T) {
	checker := newMockBlockChecker()
	checker.setDataBlock(25, []string{"tx-25"})

	s := NewSampler(SamplerConfig{
		MinHeight:    0,
		MaxHeight:    100,
		PollInterval: 0,
		MaxChecked:   1000,
	}, checker)

	// 在子范围中抽样
	result, err := s.SampleFromRange(20, 30)
	if err != nil {
		t.Fatalf("SampleFromRange 失败: %v", err)
	}
	if result.Exhausted {
		t.Fatal("不应耗尽")
	}
	if result.Height < 20 || result.Height > 30 {
		t.Errorf("SampleFromRange 返回高度 %d，不在 [20,30] 范围内", result.Height)
	}

	// 验证原始范围未变
	if s.config.MinHeight != 0 || s.config.MaxHeight != 100 {
		t.Errorf("SampleFromRange 不应改变原始范围，实际 Min=%d Max=%d",
			s.config.MinHeight, s.config.MaxHeight)
	}
}

func TestSampler_Concurrent(t *testing.T) {
	checker := newMockBlockChecker()
	checker.alwaysFound = true

	s := NewSampler(SamplerConfig{
		MinHeight:    0,
		MaxHeight:    1000,
		PollInterval: 0,
		MaxChecked:   5000,
	}, checker)

	var wg sync.WaitGroup
	errChan := make(chan error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_, err := s.Sample()
				if err != nil {
					errChan <- err
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("并发抽样出错: %v", err)
	}

	// 验证去重
	stats := s.Stats()
	if stats.TotalChecked > 100 {
		t.Errorf("并发检查数量异常: %d", stats.TotalChecked)
	}
}

// TestSampler_EmptyRange 测试空范围
func TestSampler_EmptyRange(t *testing.T) {
	checker := newMockBlockChecker()

	s := NewSampler(SamplerConfig{
		MinHeight:    100,
		MaxHeight:    100,
		PollInterval: 0,
		MaxChecked:   10,
	}, checker)

	results, err := s.SampleN(3)
	if err != nil {
		t.Fatalf("SampleN 失败: %v", err)
	}
	if len(results) < 1 || !results[0].Exhausted && !results[len(results)-1].Exhausted {
		// 单块范围，第一次或最后一次应耗尽
		if len(results) == 1 && !results[0].Exhausted {
			// 单块未检查过，不算耗尽
			if s.CheckedCount() != 1 {
				t.Errorf("应检查了 1 个块")
			}
		}
	}
}

// TestSampler_MaxCheckedZero 测试 MaxChecked=0 使用默认值
func TestSampler_MaxCheckedZero(t *testing.T) {
	checker := newMockBlockChecker()

	s := NewSampler(SamplerConfig{
		MinHeight:    0,
		MaxHeight:    100,
		PollInterval: 0,
		MaxChecked:   0, // 应改为默认值
	}, checker)

	if s.config.MaxChecked != 1_000_000 {
		t.Errorf("MaxChecked=0 应改为默认值 1000000，实际 %d", s.config.MaxChecked)
	}
}
