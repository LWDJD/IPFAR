
package main

import (
	"fmt"
	"encoding/binary"
	discovery "github.com/lwdjd/IPFAR/internal/discovery"
)

func main() {
	tests := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"json", []byte(`{"hello":"world"}`)},
		{"short_random", []byte{0x01, 0x02, 0x03}},
		{"all_zeros", make([]byte, 100)},
		{"all_0xFF", bytesRepeat(0xFF, 200)},
		{"huge_itemsNum", makeHugeItemsNum()},
		{"negative_itemsNum", makeNegativeItemsNum()},
		{"binary_blob", makeBinaryBlob()},
	}

	passed := 0
	failed := 0
	for _, t := range tests {
		func() {
			defer func() {
				if r := recover(); r != nil {
					fmt.Printf("FAIL [%s]: PANIC: %v\n", t.name, r)
					failed++
				}
			}()
			result := discovery.ClassifyByData(t.data)
			fmt.Printf("OK   [%s]: %s (len=%d)\n", t.name, result, len(t.data))
			passed++
		}()
	}
	fmt.Printf("\n%d passed, %d failed\n", passed, failed)
	if failed > 0 {
		panic("TESTS FAILED")
	}
}

func bytesRepeat(b byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}

func makeHugeItemsNum() []byte {
	// First 32 bytes encode a huge int64 that would cause make() to panic
	out := make([]byte, 64)
	// Set a huge value: 0x7FFFFFFFFFFFFFFF (max int64)
	binary.LittleEndian.PutUint64(out[:8], 0x7FFFFFFFFFFFFFFF)
	return out
}

func makeNegativeItemsNum() []byte {
	// First 32 bytes encode -1 (which would be 0xFFFFFFFFFFFFFFFF as uint64)
	out := make([]byte, 64)
	binary.LittleEndian.PutUint64(out[:8], 0xFFFFFFFFFFFFFFFF)
	return out
}

func makeBinaryBlob() []byte {
	// Random binary data that is not a valid bundle
	out := make([]byte, 500)
	for i := range out {
		out[i] = byte(i * 7 % 256)
	}
	return out
}
