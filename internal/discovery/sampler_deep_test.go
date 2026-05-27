package discovery

import (
	"sync"
	"testing"
	"time"
)

// ============================================================
// Sampler concurrent stress test
// ============================================================

func TestSampler_HighConcurrency(t *testing.T) {
	checker := newMockBlockChecker()
	checker.alwaysFound = true

	s := NewSampler(SamplerConfig{
		MinHeight:    0,
		MaxHeight:    10000,
		PollInterval: 0,
		MaxChecked:   50000,
	}, checker)

	var wg sync.WaitGroup
	numWorkers := 20
	numSamples := 50

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numSamples; j++ {
				_, err := s.Sample()
				if err != nil {
					// Exhaustion is acceptable
					continue
				}
			}
		}()
	}

	wg.Wait()

	stats := s.Stats()
	t.Logf("High concurrency stats: checked=%d, found=%d, missed=%d",
		stats.TotalChecked, stats.TotalFound, stats.TotalMissed)

	// Checked count should not exceed max checked
	if count := s.CheckedCount(); count > s.config.MaxChecked {
		t.Errorf("checked count %d exceeds MaxChecked %d", count, s.config.MaxChecked)
	}

	// No panics = pass
}

// ============================================================
// Sampler with very small range (0 blocks)
// ============================================================

func TestSampler_ZeroRange(t *testing.T) {
	checker := newMockBlockChecker()

	s := NewSampler(SamplerConfig{
		MinHeight:    0,
		MaxHeight:    0,
		PollInterval: 0,
		MaxChecked:   10,
	}, checker)

	result, err := s.Sample()
	if err != nil {
		t.Fatalf("Sample failed: %v", err)
	}

	if !result.Exhausted && result.Height > 0 {
		t.Errorf("unexpected height %d in zero-range", result.Height)
	}

	// After checking, should be exhausted
	_, err = s.SampleN(2)
	if err != nil {
		t.Fatalf("SampleN failed: %v", err)
	}
}

// ============================================================
// Sampler: UpdateRange edge cases
// ============================================================

func TestSampler_UpdateRange_Invalid(t *testing.T) {
	checker := newMockBlockChecker()

	s := NewSampler(SamplerConfig{
		MinHeight:    0,
		MaxHeight:    100,
		PollInterval: 0,
		MaxChecked:   1000,
	}, checker)

	// Set min > max (should be allowed — effective range will adjust)
	s.UpdateRange(200, 100)
	if s.config.MinHeight != 200 || s.config.MaxHeight != 100 {
		t.Logf("UpdateRange(200, 100): Min=%d Max=%d (caller should validate)", s.config.MinHeight, s.config.MaxHeight)
	}
}

// ============================================================
// Sampler: LRU eviction under concurrency
// ============================================================

func TestSampler_LRUUnderConcurrency(t *testing.T) {
	checker := newMockBlockChecker()
	checker.alwaysFound = true

	// Small MaxChecked to force LRU eviction
	s := NewSampler(SamplerConfig{
		MinHeight:    0,
		MaxHeight:    10000,
		PollInterval: 0,
		MaxChecked:   100,
	}, checker)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 30; j++ {
				s.Sample()
			}
		}()
	}

	wg.Wait()

	// checkOrder should not exceed MaxChecked
	count := s.CheckedCount()
	if count > 100 {
		t.Errorf("CheckedCount %d > MaxChecked 100 under concurrency", count)
	}

	t.Logf("LRU under concurrency: CheckedCount=%d", count)
}

// ============================================================
// Sampler: StartPolling with zero interval
// ============================================================

func TestSampler_StartPolling_ZeroInterval(t *testing.T) {
	checker := newMockBlockChecker()

	s := NewSampler(SamplerConfig{
		MinHeight:    0,
		MaxHeight:    100,
		PollInterval: 0, // Zero = no polling
		MaxChecked:   100,
	}, checker)

	// Should not panic or block
	s.StartPolling()
	time.Sleep(50 * time.Millisecond)
	s.Stop()
}

// ============================================================
// Sampler: Poll with updated range
// ============================================================

func TestSampler_PollingWithUpdates(t *testing.T) {
	checker := newMockBlockChecker()
	checker.setDataBlock(50, []string{"tx-50"})

	s := NewSampler(SamplerConfig{
		MinHeight:    0,
		MaxHeight:    10,
		PollInterval: 50 * time.Millisecond,
		MaxChecked:   100,
	}, checker)

	s.StartPolling()

	// Update max height after start
	time.Sleep(30 * time.Millisecond)
	s.UpdateMaxHeight(50)

	// Let poll run
	time.Sleep(100 * time.Millisecond)
	s.Stop()

	stats := s.Stats()
	t.Logf("Polling stats: checked=%d, last_height=%d", stats.TotalChecked, stats.LastHeight)
}

// ============================================================
// Sampler: GetAllFound after many samples
// ============================================================

func TestSampler_GetAllFound_ManyHits(t *testing.T) {
	checker := newMockBlockChecker()

	// Set many blocks with data
	for h := uint64(0); h < 100; h += 10 {
		checker.setDataBlock(h, []string{"tx-" + formatUint64(h)})
	}

	s := NewSampler(SamplerConfig{
		MinHeight:    0,
		MaxHeight:    99,
		PollInterval: 0,
		MaxChecked:   1000,
	}, checker)

	// Sample until exhausted
	for i := 0; i < 200; i++ {
		result, err := s.Sample()
		if err != nil {
			t.Fatalf("Sample failed: %v", err)
		}
		if result.Exhausted {
			break
		}
	}

	allFound := s.GetAllFound()
	t.Logf("Found data at %d heights", len(allFound))

	// Should have found all 10 preset blocks
	if len(allFound) < 10 {
		t.Errorf("expected at least 10 found heights, got %d", len(allFound))
	}
}

func formatUint64(n uint64) string {
	if n == 0 {
		return "0"
	}
	result := make([]byte, 0, 20)
	for n > 0 {
		result = append([]byte{byte('0' + n%10)}, result...)
		n /= 10
	}
	return string(result)
}

// ============================================================
// Sampler: FoundAt with empty sampler
// ============================================================

func TestSampler_FoundAt_EmptySampler(t *testing.T) {
	checker := newMockBlockChecker()

	s := NewSampler(SamplerConfig{
		MinHeight:    0,
		MaxHeight:    10,
		PollInterval: 0,
		MaxChecked:   100,
	}, checker)

	_, ok := s.FoundAt(5)
	if ok {
		t.Error("FoundAt should return false for unchecked height")
	}
}

// ============================================================
// Sampler: Stop called multiple times
// ============================================================

func TestSampler_StopMultiple(t *testing.T) {
	checker := newMockBlockChecker()

	s := NewSampler(SamplerConfig{
		MinHeight:    0,
		MaxHeight:    100,
		PollInterval: 10 * time.Millisecond,
		MaxChecked:   100,
	}, checker)

	s.StartPolling()
	time.Sleep(20 * time.Millisecond)

	// Stop multiple times — should not panic
	s.Stop()
	s.Stop()
	s.Stop()
}

// ============================================================
// Sampler: SampleN with n=0 (should error)
// ============================================================

func TestSampler_SampleN_Zero(t *testing.T) {
	checker := newMockBlockChecker()

	s := NewSampler(SamplerConfig{
		MinHeight:    0,
		MaxHeight:    100,
		PollInterval: 0,
		MaxChecked:   100,
	}, checker)

	_, err := s.SampleN(0)
	if err == nil {
		t.Fatal("SampleN(0) should error")
	}
}

func TestSampler_SampleN_Negative(t *testing.T) {
	checker := newMockBlockChecker()

	s := NewSampler(SamplerConfig{
		MinHeight:    0,
		MaxHeight:    100,
		PollInterval: 0,
		MaxChecked:   100,
	}, checker)

	_, err := s.SampleN(-1)
	if err == nil {
		t.Fatal("SampleN(-1) should error")
	}
}

// ============================================================
// Sampler: CheckedCount consistency
// ============================================================

func TestSampler_CheckedCountConsistency(t *testing.T) {
	checker := newMockBlockChecker()
	checker.alwaysFound = true

	s := NewSampler(SamplerConfig{
		MinHeight:    0,
		MaxHeight:    50,
		PollInterval: 0,
		MaxChecked:   100,
	}, checker)

	initialCount := s.CheckedCount()
	if initialCount != 0 {
		t.Errorf("initial count should be 0, got %d", initialCount)
	}

	n := 10
	results, err := s.SampleN(n)
	if err != nil {
		t.Fatalf("SampleN failed: %v", err)
	}

	// Check no exhaustion early
	for i, r := range results {
		if r.Exhausted && i < n-1 {
			t.Errorf("exhausted too early at sample %d", i)
		}
	}

	finalCount := s.CheckedCount()
	if finalCount < n {
		t.Errorf("checked count %d < samples %d", finalCount, n)
	}
}
