package discovery

import (
	"bytes"
	"testing"
)

func TestNewBitlist(t *testing.T) {
	bl := NewBitlist(63)
	if len(bl) != 1 {
		t.Errorf("NewBitlist(63) length = %d, want 1", len(bl))
	}
	if bl.MaxHeight() != 63 {
		t.Errorf("NewBitlist(63) MaxHeight = %d, want 63", bl.MaxHeight())
	}

	bl2 := NewBitlist(64)
	if len(bl2) != 2 {
		t.Errorf("NewBitlist(64) length = %d, want 2", len(bl2))
	}
	if bl2.MaxHeight() != 127 {
		t.Errorf("NewBitlist(64) MaxHeight = %d, want 127", bl2.MaxHeight())
	}

	bl3 := NewBitlist(0)
	if len(bl3) != 0 {
		t.Errorf("NewBitlist(0) length = %d, want 0", len(bl3))
	}
}

func TestBitlist_SetGet(t *testing.T) {
	bl := NewBitlist(127)

	// Set and Get
	if err := bl.Set(0); err != nil {
		t.Fatalf("Set(0) failed: %v", err)
	}
	if !bl.Get(0) {
		t.Error("Get(0) should be true after Set(0)")
	}

	if err := bl.Set(63); err != nil {
		t.Fatalf("Set(63) failed: %v", err)
	}
	if !bl.Get(63) {
		t.Error("Get(63) should be true")
	}

	if err := bl.Set(64); err != nil {
		t.Fatalf("Set(64) failed: %v", err)
	}
	if !bl.Get(64) {
		t.Error("Get(64) should be true")
	}

	if err := bl.Set(127); err != nil {
		t.Fatalf("Set(127) failed: %v", err)
	}
	if !bl.Get(127) {
		t.Error("Get(127) should be true")
	}

	// Out of range Get should return false
	if bl.Get(128) {
		t.Error("Get(128) out of range should return false")
	}

	// Out of range Set should error
	if err := bl.Set(128); err == nil {
		t.Error("Set(128) should error (out of range)")
	}
}

func TestBitlist_Unset(t *testing.T) {
	bl := NewBitlist(63)
	bl.Set(10)
	if !bl.Get(10) {
		t.Fatal("Set(10) failed")
	}

	if err := bl.Unset(10); err != nil {
		t.Fatalf("Unset(10) failed: %v", err)
	}
	if bl.Get(10) {
		t.Error("Get(10) should be false after Unset")
	}

	// Unset out of range
	if err := bl.Unset(64); err == nil {
		t.Error("Unset(64) should error (out of range)")
	}
}

func TestBitlist_Count(t *testing.T) {
	bl := NewBitlist(127)

	if bl.Count() != 0 {
		t.Errorf("empty bitlist Count = %d, want 0", bl.Count())
	}

	bl.Set(0)
	bl.Set(63)
	bl.Set(64)
	bl.Set(127)

	if bl.Count() != 4 {
		t.Errorf("Count = %d, want 4", bl.Count())
	}
}

func TestBitlist_ToBytes_FromBytes(t *testing.T) {
	bl := NewBitlist(127)
	bl.Set(0)
	bl.Set(63)
	bl.Set(64)
	bl.Set(127)

	data := bl.ToBytes()
	if len(data) != 16 { // 2 uint64 = 16 bytes
		t.Errorf("ToBytes length = %d, want 16", len(data))
	}

	bl2, err := NewBitlistFromBytes(data)
	if err != nil {
		t.Fatalf("NewBitlistFromBytes failed: %v", err)
	}

	if !bl.Equal(bl2) {
		t.Error("round-trip failed: bitlists not equal")
	}
}

func TestNewBitlistFromBytes_Errors(t *testing.T) {
	// Empty
	_, err := NewBitlistFromBytes([]byte{})
	if err != ErrEmptyBitlist {
		t.Errorf("empty bytes: want ErrEmptyBitlist, got %v", err)
	}

	// Invalid length
	_, err = NewBitlistFromBytes([]byte{1, 2, 3})
	if err == nil {
		t.Error("length 3 should error")
	}
}

func TestBitlist_Or(t *testing.T) {
	bl1 := NewBitlist(63)
	bl1.Set(0)
	bl1.Set(10)

	bl2 := NewBitlist(63)
	bl2.Set(10)
	bl2.Set(20)

	result := bl1.Or(bl2)
	if !result.Get(0) {
		t.Error("Or result should have bit 0")
	}
	if !result.Get(10) {
		t.Error("Or result should have bit 10")
	}
	if !result.Get(20) {
		t.Error("Or result should have bit 20")
	}
	if result.Count() != 3 {
		t.Errorf("Or Count = %d, want 3", result.Count())
	}
}

func TestBitlist_Or_DifferentLength(t *testing.T) {
	bl1 := NewBitlist(63)  // 1 uint64
	bl2 := NewBitlist(127) // 2 uint64

	bl1.Set(0)
	bl2.Set(100)

	result := bl1.Or(bl2)
	if len(result) != 2 {
		t.Errorf("Or length = %d, want 2", len(result))
	}
	if !result.Get(0) {
		t.Error("should have bit 0")
	}
	if !result.Get(100) {
		t.Error("should have bit 100")
	}
}

func TestBitlist_And(t *testing.T) {
	bl1 := NewBitlist(63)
	bl1.Set(0)
	bl1.Set(10)
	bl1.Set(20)

	bl2 := NewBitlist(63)
	bl2.Set(10)
	bl2.Set(20)
	bl2.Set(30)

	result := bl1.And(bl2)
	if result.Get(0) {
		t.Error("And result should not have bit 0")
	}
	if !result.Get(10) {
		t.Error("And result should have bit 10")
	}
	if !result.Get(20) {
		t.Error("And result should have bit 20")
	}
	if result.Get(30) {
		t.Error("And result should not have bit 30")
	}
	if result.Count() != 2 {
		t.Errorf("And Count = %d, want 2", result.Count())
	}
}

func TestBitlist_Xor(t *testing.T) {
	bl1 := NewBitlist(63)
	bl1.Set(0)
	bl1.Set(10)

	bl2 := NewBitlist(63)
	bl2.Set(10)
	bl2.Set(20)

	result := bl1.Xor(bl2)
	if !result.Get(0) {
		t.Error("Xor result should have bit 0 (only in bl1)")
	}
	if result.Get(10) {
		t.Error("Xor result should not have bit 10 (in both)")
	}
	if !result.Get(20) {
		t.Error("Xor result should have bit 20 (only in bl2)")
	}
	if result.Count() != 2 {
		t.Errorf("Xor Count = %d, want 2", result.Count())
	}
}

func TestBitlist_Clone(t *testing.T) {
	bl := NewBitlist(63)
	bl.Set(5)
	bl.Set(15)

	clone := bl.Clone()
	if !bl.Equal(clone) {
		t.Error("Clone should be equal")
	}

	// Modify original, clone should be unaffected
	bl.Set(25)
	if clone.Get(25) {
		t.Error("Clone should not be affected by original modification")
	}
}

func TestBitlist_SetRange(t *testing.T) {
	bl := NewBitlist(127)
	err := bl.SetRange(10, 20)
	if err != nil {
		t.Fatalf("SetRange(10,20) failed: %v", err)
	}

	for h := uint64(10); h <= 20; h++ {
		if !bl.Get(h) {
			t.Errorf("Get(%d) should be true after SetRange(10,20)", h)
		}
	}

	if bl.Get(9) {
		t.Error("Get(9) should be false")
	}
	if bl.Get(21) {
		t.Error("Get(21) should be false")
	}

	if bl.Count() != 11 {
		t.Errorf("Count = %d, want 11", bl.Count())
	}
}

func TestBitlist_SetRange_Errors(t *testing.T) {
	bl := NewBitlist(63)
	if err := bl.SetRange(10, 100); err == nil {
		t.Error("SetRange with endHeight out of range should error")
	}
	if err := bl.SetRange(20, 10); err == nil {
		t.Error("SetRange with startHeight > endHeight should error")
	}
}

func TestBitlist_Ones(t *testing.T) {
	bl := NewBitlist(127)
	bl.Set(0)
	bl.Set(63)
	bl.Set(64)
	bl.Set(127)

	ones := bl.Ones()
	if len(ones) != 4 {
		t.Errorf("Ones length = %d, want 4", len(ones))
	}

	expected := []uint64{0, 63, 64, 127}
	for i, h := range ones {
		if h != expected[i] {
			t.Errorf("Ones[%d] = %d, want %d", i, h, expected[i])
		}
	}
}

func TestBitlist_Zeros(t *testing.T) {
	bl := NewBitlist(9) // 0-63 range
	bl.Set(0)
	bl.Set(5)

	zeros := bl.Zeros()
	if len(zeros) != 62 { // 64 total - 2 set
		t.Errorf("Zeros length = %d, want 62", len(zeros))
	}

	// Check a few
	for _, h := range zeros {
		if h == 0 || h == 5 {
			t.Errorf("height %d should not be in zeros", h)
		}
	}
}

func TestBitlist_Equal(t *testing.T) {
	bl1 := NewBitlist(63)
	bl1.Set(10)

	bl2 := NewBitlist(63)
	bl2.Set(10)

	if !bl1.Equal(bl2) {
		t.Error("identical bitlists should be equal")
	}

	bl2.Set(20)
	if bl1.Equal(bl2) {
		t.Error("different bitlists should not be equal")
	}

	bl3 := NewBitlist(127) // different length
	if bl1.Equal(bl3) {
		t.Error("different length bitlists should not be equal")
	}
}

func TestBitlist_IsEmpty(t *testing.T) {
	bl := NewBitlist(63)
	if !bl.IsEmpty() {
		t.Error("new bitlist should be empty")
	}

	bl.Set(10)
	if bl.IsEmpty() {
		t.Error("bitlist with data should not be empty")
	}
}

func TestBitlist_Extend(t *testing.T) {
	bl := NewBitlist(63)
	bl.Set(10)

	// Extend to larger
	ext := bl.Extend(127)
	if len(ext) != 2 {
		t.Errorf("Extend(127) length = %d, want 2", len(ext))
	}
	if !ext.Get(10) {
		t.Error("extended should keep bit 10")
	}
	if ext.Get(100) {
		t.Error("extended should not have bit 100")
	}

	// Extend to smaller (truncate)
	trunc := ext.Extend(31)
	if len(trunc) != 1 {
		t.Errorf("Extend(31) length = %d, want 1", len(trunc))
	}
	if !trunc.Get(10) {
		t.Error("truncated should keep bit 10")
	}
}

func TestMerge(t *testing.T) {
	bl1 := NewBitlist(63)
	bl1.Set(0)
	bl2 := NewBitlist(63)
	bl2.Set(10)
	bl3 := NewBitlist(63)
	bl3.Set(20)

	result := Merge(bl1, bl2, bl3)
	if result.Count() != 3 {
		t.Errorf("Merge Count = %d, want 3", result.Count())
	}
	if !result.Get(0) || !result.Get(10) || !result.Get(20) {
		t.Error("Merge should contain all bits")
	}
}

func TestMerge_Empty(t *testing.T) {
	result := Merge()
	if result != nil {
		t.Error("Merge() should return nil")
	}
}

func TestIntersect(t *testing.T) {
	bl1 := NewBitlist(63)
	bl1.Set(0)
	bl1.Set(10)
	bl1.Set(20)

	bl2 := NewBitlist(63)
	bl2.Set(10)
	bl2.Set(20)
	bl2.Set(30)

	bl3 := NewBitlist(63)
	bl3.Set(20)
	bl3.Set(30)
	bl3.Set(40)

	result := Intersect(bl1, bl2, bl3)
	if result.Count() != 1 {
		t.Errorf("Intersect Count = %d, want 1", result.Count())
	}
	if !result.Get(20) {
		t.Error("Intersect should have bit 20")
	}
}

func TestIntersect_Empty(t *testing.T) {
	result := Intersect()
	if result != nil {
		t.Error("Intersect() should return nil")
	}
}

func TestFromHeights(t *testing.T) {
	bl, err := FromHeights(127, 0, 10, 63, 64, 127)
	if err != nil {
		t.Fatalf("FromHeights failed: %v", err)
	}

	expected := []uint64{0, 10, 63, 64, 127}
	for _, h := range expected {
		if !bl.Get(h) {
			t.Errorf("Get(%d) should be true", h)
		}
	}
	if bl.Count() != 5 {
		t.Errorf("Count = %d, want 5", bl.Count())
	}
}

func TestFromHeights_Error(t *testing.T) {
	_, err := FromHeights(63, 0, 100) // 100 > 63
	if err == nil {
		t.Error("FromHeights with out-of-range height should error")
	}
}

func TestCompact(t *testing.T) {
	bl := NewBitlist(127)
	bl.Set(0)
	// bits 64-127 are all zero, so second uint64 is zero

	compacted := bl.Compact()
	if len(compacted) != 1 {
		t.Errorf("Compact length = %d, want 1", len(compacted))
	}
	if !compacted.Get(0) {
		t.Error("compacted should have bit 0")
	}

	// All zero
	bl2 := NewBitlist(63)
	compacted2 := bl2.Compact()
	if len(compacted2) != 0 {
		t.Errorf("all-zero Compact length = %d, want 0", len(compacted2))
	}
}

func TestBitlist_Empty_ToBytes(t *testing.T) {
	var bl Bitlist
	data := bl.ToBytes()
	if data != nil {
		t.Error("nil Bitlist ToBytes should return nil")
	}
}

func TestBitlist_Empty_Get(t *testing.T) {
	var bl Bitlist
	if bl.Get(0) {
		t.Error("nil Bitlist Get(0) should return false")
	}
	if bl.Count() != 0 {
		t.Error("nil Bitlist Count should be 0")
	}
	if bl.MaxHeight() != 0 {
		t.Error("nil Bitlist MaxHeight should be 0")
	}
}

func TestBitlist_Empty_Ones(t *testing.T) {
	var bl Bitlist
	ones := bl.Ones()
	if len(ones) != 0 {
		t.Errorf("nil Bitlist Ones should return empty slice, got len=%d", len(ones))
	}
}

func TestBitlist_Empty_Zeros(t *testing.T) {
	var bl Bitlist
	zeros := bl.Zeros()
	if len(zeros) != 0 {
		t.Errorf("nil Bitlist Zeros should return empty slice, got len=%d", len(zeros))
	}
}

func TestBitlist_Empty_IsEmpty(t *testing.T) {
	var bl Bitlist
	if !bl.IsEmpty() {
		t.Error("nil Bitlist should be empty")
	}
}

func TestBitlist_LargeRange(t *testing.T) {
	// Test with a bitlist representing 1 million blocks
	// This should use ~125 KB
	maxHeight := uint64(1_000_000)
	bl := NewBitlist(maxHeight)

	expectedSize := (maxHeight + 64) / 64
	if bl.Size() != int(expectedSize) {
		t.Errorf("Size = %d, want %d", bl.Size(), expectedSize)
	}

	// Set some bits at various positions
	bl.Set(0)
	bl.Set(500_000)
	bl.Set(1_000_000)

	if !bl.Get(0) || !bl.Get(500_000) || !bl.Get(1_000_000) {
		t.Error("Large range Get failed")
	}

	if bl.Count() != 3 {
		t.Errorf("Large range Count = %d, want 3", bl.Count())
	}

	// Verify byte round-trip
	data := bl.ToBytes()
	bl2, err := NewBitlistFromBytes(data)
	if err != nil {
		t.Fatalf("Large range round-trip failed: %v", err)
	}
	if !bl.Equal(bl2) {
		t.Error("Large range round-trip not equal")
	}
}

func TestBitlist_Bytes_Endianness(t *testing.T) {
	// Test deterministic encoding
	bl := NewBitlist(63)
	bl.Set(0) // bit 0 of first uint64 → value 0x0000000000000001

	data := bl.ToBytes()
	// BigEndian: most significant byte first
	// 0x0000000000000001 → [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]
	expected := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}
	if !bytes.Equal(data, expected) {
		t.Errorf("encoding = %x, want %x", data, expected)
	}

	bl2, err := NewBitlistFromBytes(data)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if !bl2.Get(0) {
		t.Error("decoded bitlist should have bit 0 set")
	}

	// Also test bit 63 (MSB of first uint64)
	bl3 := NewBitlist(63)
	bl3.Set(63) // bit 63 → 0x8000000000000000
	data3 := bl3.ToBytes()
	// BigEndian: [0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
	expected3 := []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	if !bytes.Equal(data3, expected3) {
		t.Errorf("encoding of bit 63 = %x, want %x", data3, expected3)
	}
}
