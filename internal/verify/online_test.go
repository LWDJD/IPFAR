package verify

import (
	"encoding/binary"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LWDJD/ipfar-sdk/verify/ipfs"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"

	"github.com/lwdjd/IPFAR/internal/download"
)

// TestNewOnlineVerifier 测试创建在线验证器
func TestNewOnlineVerifier(t *testing.T) {
	v := NewOnlineVerifier(DefaultOnlineVerifierConfig())
	if v == nil {
		t.Fatal("NewOnlineVerifier returned nil")
	}
	if v.gateway == nil {
		t.Fatal("gateway is nil")
	}
	if v.sema == nil {
		t.Fatal("sema is nil")
	}
	if cap(v.sema) != 4 {
		t.Errorf("sema capacity = %d, want 4", cap(v.sema))
	}
}

func TestNewOnlineVerifier_WithGateway(t *testing.T) {
	gw := download.NewGateway(download.DefaultGatewayConfig())
	cfg := OnlineVerifierConfig{
		Gateway:        gw,
		SampleCount:    10,
		MaxConcurrency: 8,
	}
	v := NewOnlineVerifier(cfg)
	if v.gateway != gw {
		t.Error("gateway should be the same instance")
	}
	if v.config.SampleCount != 10 {
		t.Errorf("SampleCount = %d, want 10", v.config.SampleCount)
	}
	if cap(v.sema) != 8 {
		t.Errorf("sema capacity = %d, want 8", cap(v.sema))
	}
}

// TestReadVersion 测试 CAR 版本读取
func TestReadVersion(t *testing.T) {
	v := NewOnlineVerifier(DefaultOnlineVerifierConfig())

	// CAR v2 pragma bytes
	carV2Pragma := []byte{
		0x0a, 0xa1, 0x67, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x02,
	}

	ver, err := v.readVersion(carV2Pragma)
	if err != nil {
		t.Fatalf("readVersion failed: %v", err)
	}
	if ver != 2 {
		t.Errorf("version = %d, want 2", ver)
	}

	// CAR v1 needs different data; just check it doesn't panic
	carV1Data := []byte{0x01}
	_, err = v.readVersion(carV1Data)
	if err == nil {
		t.Log("CAR v1 detection (may or may not fail depending on data)")
	}
}

// TestParseCarV2Header 测试 CAR v2 header 解析
func TestParseCarV2Header(t *testing.T) {
	v := NewOnlineVerifier(DefaultOnlineVerifierConfig())

	// 构造一个 mock CAR v2 header
	pragma := []byte{
		0x0a, 0xa1, 0x67, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x02,
	}

	// Header: Characteristics(16) + DataOffset(8) + DataSize(8) + IndexOffset(8) = 40 bytes
	header := make([]byte, 40)
	// DataOffset = PragmaSize(11) + HeaderSize(40) = 51
	binary.LittleEndian.PutUint64(header[16:24], 51)
	// DataSize = 200
	binary.LittleEndian.PutUint64(header[24:32], 200)
	// IndexOffset = 51 + 200 = 251
	binary.LittleEndian.PutUint64(header[32:40], 251)

	fullHeader := append(pragma, header...)

	hdr, err := v.parseCarV2Header(fullHeader)
	if err != nil {
		t.Fatalf("parseCarV2Header failed: %v", err)
	}
	if hdr.DataOffset != 51 {
		t.Errorf("DataOffset = %d, want 51", hdr.DataOffset)
	}
	if hdr.DataSize != 200 {
		t.Errorf("DataSize = %d, want 200", hdr.DataSize)
	}
	if hdr.IndexOffset != 251 {
		t.Errorf("IndexOffset = %d, want 251", hdr.IndexOffset)
	}
	if !hdr.HasIndex() {
		t.Error("HasIndex should be true")
	}
}

// TestParseCarV2Header_NoIndex 测试无索引的 CAR v2 header
func TestParseCarV2Header_NoIndex(t *testing.T) {
	v := NewOnlineVerifier(DefaultOnlineVerifierConfig())

	pragma := []byte{
		0x0a, 0xa1, 0x67, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x02,
	}
	header := make([]byte, 40)
	binary.LittleEndian.PutUint64(header[16:24], 51)
	binary.LittleEndian.PutUint64(header[24:32], 200)
	// IndexOffset = 0 (no index)
	binary.LittleEndian.PutUint64(header[32:40], 0)

	fullHeader := append(pragma, header...)

	hdr, err := v.parseCarV2Header(fullHeader)
	if err != nil {
		t.Fatalf("parseCarV2Header failed: %v", err)
	}
	if hdr.HasIndex() {
		t.Error("HasIndex should be false")
	}
}

// buildMockIndexSorted 构建一个 CarIndexSorted 格式的 mock 索引
func buildMockIndexSorted(entries []IndexEntry) []byte {
	// 按 digest 长度分组
	byWidth := make(map[uint32][]IndexEntry)
	for _, e := range entries {
		w := uint32(len(e.MultihashDigest)) + 8
		byWidth[w] = append(byWidth[w], e)
	}

	// codec
	var buf []byte
	codecBuf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(codecBuf, uint64(multicodec.CarIndexSorted))
	buf = append(buf, codecBuf[:n]...)

	// bucket count
	countBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(countBuf, uint32(len(byWidth)))
	buf = append(buf, countBuf...)

	for width, ents := range byWidth {
		// width
		wBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(wBuf, width)
		buf = append(buf, wBuf...)

		// data length
		dataLen := uint64(len(ents)) * uint64(width)
		dBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(dBuf, dataLen)
		buf = append(buf, dBuf...)

		// entries
		digestLen := int(width) - 8
		for _, e := range ents {
			entry := make([]byte, width)
			copy(entry[:digestLen], e.MultihashDigest)
			binary.LittleEndian.PutUint64(entry[digestLen:], e.Offset)
			buf = append(buf, entry...)
		}
	}

	return buf
}

// TestParseIndexSorted 测试 CarIndexSorted 索引解析
func TestParseIndexSorted(t *testing.T) {
	v := NewOnlineVerifier(DefaultOnlineVerifierConfig())

	entries := []IndexEntry{
		{MultihashDigest: []byte{0x01, 0x02, 0x03, 0x04}, Offset: 100},
		{MultihashDigest: []byte{0x05, 0x06, 0x07, 0x08}, Offset: 200},
		{MultihashDigest: []byte{0x09, 0x0a, 0x0b, 0x0c}, Offset: 300},
	}

	indexData := buildMockIndexSorted(entries)
	parsed, err := v.parseIndex(indexData)
	if err != nil {
		t.Fatalf("parseIndex failed: %v", err)
	}
	if len(parsed) != 3 {
		t.Fatalf("parsed %d entries, want 3", len(parsed))
	}
	for i, e := range parsed {
		if e.Offset != entries[i].Offset {
			t.Errorf("entry %d offset = %d, want %d", i, e.Offset, entries[i].Offset)
		}
	}
}

// TestParseIndex_Empty 测试空索引
func TestParseIndex_Empty(t *testing.T) {
	v := NewOnlineVerifier(DefaultOnlineVerifierConfig())

	// 空索引：codec + 0 buckets
	var buf []byte
	codecBuf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(codecBuf, uint64(multicodec.CarIndexSorted))
	buf = append(buf, codecBuf[:n]...)
	countBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(countBuf, 0)
	buf = append(buf, countBuf...)

	parsed, err := v.parseIndex(buf)
	if err != nil {
		t.Fatalf("parseIndex failed: %v", err)
	}
	if len(parsed) != 0 {
		t.Errorf("parsed %d entries, want 0", len(parsed))
	}
}

// TestRandomSample 测试随机采样逻辑
func TestRandomSample(t *testing.T) {
	v := NewOnlineVerifier(DefaultOnlineVerifierConfig())

	entries := make([]IndexEntry, 100)
	for i := range entries {
		entries[i] = IndexEntry{
			MultihashDigest: []byte{byte(i)},
			Offset:          uint64(i * 10),
		}
	}

	// 采样少于总数
	sampled := v.randomSample(entries, 10)
	if len(sampled) != 10 {
		t.Errorf("sampled %d entries, want 10", len(sampled))
	}

	// 去重检查
	seen := make(map[uint64]bool)
	for _, e := range sampled {
		if seen[e.Offset] {
			t.Errorf("duplicate entry in sample: offset=%d", e.Offset)
		}
		seen[e.Offset] = true
	}

	// 采样全部
	sampledAll := v.randomSample(entries, 200)
	if len(sampledAll) != 100 {
		t.Errorf("sampled %d entries, want 100", len(sampledAll))
	}
}

// ============================================================
// 集成测试：使用 mock HTTP 服务器
// ============================================================

// mockCARv2Server 创建模拟 CAR v2 文件的 HTTP 服务器
type mockCARv2Server struct {
	server     *httptest.Server
	carData    []byte
	blockCIDs  []cid.Cid
	blockData  [][]byte
	indexData  []byte
	rangeCount atomic.Int32
}

// setupMockCARv2Server 搭建完整 mock CAR v2 服务器
// 返回 server 和用于测试的 mock 数据
func setupMockCARv2Server(t *testing.T) *mockCARv2Server {
	t.Helper()

	m := &mockCARv2Server{}

	// 创建一些测试 block
	numBlocks := 20

	// 使用原始 multihash (identity) 简化测试
	for i := 0; i < numBlocks; i++ {
		data := []byte(fmt.Sprintf("block-data-%04d", i))
		mh, err := multihash.Sum(data, multihash.IDENTITY, -1)
		if err != nil {
			t.Fatalf("multihash.Sum failed: %v", err)
		}
		c := cid.NewCidV1(cid.Raw, mh)
		m.blockCIDs = append(m.blockCIDs, c)
		m.blockData = append(m.blockData, data)
	}

	// 构建 CAR v1 数据区
	// CAR v1 header: version=1, roots=0
	var carV1Data []byte

	// Version varint
	verBuf := make([]byte, binary.MaxVarintLen64)
	verN := binary.PutUvarint(verBuf, 1)
	carV1Data = append(carV1Data, verBuf[:verN]...)

	// Root count = 0
	rootCountBuf := make([]byte, binary.MaxVarintLen64)
	rcN := binary.PutUvarint(rootCountBuf, 0)
	carV1Data = append(carV1Data, rootCountBuf[:rcN]...)

	// Blocks
	offsets := make([]IndexEntry, 0, numBlocks)
	for i := 0; i < numBlocks; i++ {
		blockOffset := uint64(len(carV1Data))

		cidBytes := m.blockCIDs[i].Bytes()
		sectionLen := uint64(len(cidBytes) + len(m.blockData[i]))

		// section length varint
		slBuf := make([]byte, binary.MaxVarintLen64)
		slN := binary.PutUvarint(slBuf, sectionLen)
		carV1Data = append(carV1Data, slBuf[:slN]...)

		// CID
		carV1Data = append(carV1Data, cidBytes...)

		// Data
		carV1Data = append(carV1Data, m.blockData[i]...)

		// 记录索引条目
		dmh, err := multihash.Decode(m.blockCIDs[i].Hash())
		if err != nil {
			t.Fatalf("multihash decode failed: %v", err)
		}
		offsets = append(offsets, IndexEntry{
			MultihashDigest: dmh.Digest,
			Offset:          blockOffset,
		})
	}

	// 构建索引
	m.indexData = buildMockIndexSorted(offsets)

	// 构建完整 CAR v2
	pragma := []byte{
		0x0a, 0xa1, 0x67, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x02,
	}

	// CAR v2 header (40 bytes)
	carV2Header := make([]byte, 40)
	dataOffset := uint64(len(pragma) + 40) // pragma + header
	dataSize := uint64(len(carV1Data))
	indexOffset := dataOffset + dataSize

	binary.LittleEndian.PutUint64(carV2Header[16:24], dataOffset)
	binary.LittleEndian.PutUint64(carV2Header[24:32], dataSize)
	binary.LittleEndian.PutUint64(carV2Header[32:40], indexOffset)

	m.carData = append(pragma, carV2Header...)
	m.carData = append(m.carData, carV1Data...)
	m.carData = append(m.carData, m.indexData...)

	// 创建 HTTP mock server
	m.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 解析 Range header
		rangeHeader := r.Header.Get("Range")
		if rangeHeader != "" {
			m.rangeCount.Add(1)
			var rangeStart, rangeEnd int64
			fmt.Sscanf(rangeHeader, "bytes=%d-%d", &rangeStart, &rangeEnd)

			if rangeStart < 0 {
				rangeStart = 0
			}
			if rangeEnd >= int64(len(m.carData)) {
				rangeEnd = int64(len(m.carData)) - 1
			}

			w.Header().Set("Content-Type", "application/vnd.ipld.car")
			w.Header().Set("Content-Range",
				fmt.Sprintf("bytes %d-%d/%d", rangeStart, rangeEnd, len(m.carData)))
			w.WriteHeader(http.StatusPartialContent)
			w.Write(m.carData[rangeStart : rangeEnd+1])
			return
		}

		// HEAD 请求
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(m.carData)))
			w.WriteHeader(http.StatusOK)
			return
		}

		// 完整下载
		w.Header().Set("Content-Type", "application/vnd.ipld.car")
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(m.carData)))
		w.WriteHeader(http.StatusOK)
		w.Write(m.carData)
	}))

	return m
}

func (m *mockCARv2Server) Close() {
	m.server.Close()
}

func (m *mockCARv2Server) URL() string {
	return m.server.URL
}

// TestOnlineVerifier_Verify_WithMockServer 端到端测试：在线验证
func TestOnlineVerifier_Verify_WithMockServer(t *testing.T) {
	mock := setupMockCARv2Server(t)
	defer mock.Close()

	gw := download.NewGateway(download.GatewayConfig{
		URLs:       []string{mock.URL()},
		Timeout:    5 * time.Second, // need time import
		MaxRetries: 1,
		RetryDelay: 10 * time.Millisecond,
	})

	v := NewOnlineVerifier(OnlineVerifierConfig{
		Gateway:        gw,
		SampleCount:    5,
		MaxConcurrency: 4,
	})

	result, err := v.Verify("any-txid")
	if err != nil {
		t.Fatalf("Verify failed: %v", err)
	}
	if !result.Passed {
		t.Errorf("Verify should pass, got Passed=%v", result.Passed)
	}
	if result.TotalBlocks != 20 {
		t.Errorf("TotalBlocks = %d, want 20", result.TotalBlocks)
	}
	if result.SampledBlocks != 5 {
		t.Errorf("SampledBlocks = %d, want 5", result.SampledBlocks)
	}
	if result.VerifiedBlocks != 5 {
		t.Errorf("VerifiedBlocks = %d, want 5 (all should verify)", result.VerifiedBlocks)
	}
	if result.FailedBlocks != 0 {
		t.Errorf("FailedBlocks = %d, want 0", result.FailedBlocks)
	}

	t.Logf("Range requests made: %d", mock.rangeCount.Load())
}

// TestOnlineVerifier_Verify_AllBlocks 测试采样全部 block
func TestOnlineVerifier_Verify_AllBlocks(t *testing.T) {
	mock := setupMockCARv2Server(t)
	defer mock.Close()

	gw := download.NewGateway(download.GatewayConfig{
		URLs:       []string{mock.URL()},
		Timeout:    5 * time.Second,
		MaxRetries: 1,
		RetryDelay: 10 * time.Millisecond,
	})

	v := NewOnlineVerifier(OnlineVerifierConfig{
		Gateway:        gw,
		SampleCount:    20, // 采样全部
		MaxConcurrency: 4,
	})

	result, err := v.Verify("any-txid")
	if err != nil {
		t.Fatalf("Verify failed: %v", err)
	}
	if !result.Passed {
		t.Errorf("Verify should pass")
	}
	if result.SampledBlocks != 20 {
		t.Errorf("SampledBlocks = %d, want 20", result.SampledBlocks)
	}
	if result.VerifiedBlocks != 20 {
		t.Errorf("VerifiedBlocks = %d, want 20", result.VerifiedBlocks)
	}
}

// TestOnlineVerifier_VerifyWithSampleCount 测试自定义采样数量
func TestOnlineVerifier_VerifyWithSampleCount(t *testing.T) {
	mock := setupMockCARv2Server(t)
	defer mock.Close()

	gw := download.NewGateway(download.GatewayConfig{
		URLs:       []string{mock.URL()},
		Timeout:    5 * time.Second,
		MaxRetries: 1,
		RetryDelay: 10 * time.Millisecond,
	})

	v := NewOnlineVerifier(OnlineVerifierConfig{
		Gateway:        gw,
		SampleCount:    5,
		MaxConcurrency: 4,
	})

	result, err := v.VerifyWithSampleCount("any-txid", 3)
	if err != nil {
		t.Fatalf("VerifyWithSampleCount failed: %v", err)
	}
	if result.SampledBlocks != 3 {
		t.Errorf("SampledBlocks = %d, want 3", result.SampledBlocks)
	}
}

// TestVerifyBlock 测试单个 block 验证
func TestVerifyBlock(t *testing.T) {
	mock := setupMockCARv2Server(t)
	defer mock.Close()

	gw := download.NewGateway(download.GatewayConfig{
		URLs:       []string{mock.URL()},
		Timeout:    5 * time.Second,
		MaxRetries: 1,
		RetryDelay: 10 * time.Millisecond,
	})

	v := NewOnlineVerifier(OnlineVerifierConfig{
		Gateway:        gw,
		SampleCount:    5,
		MaxConcurrency: 4,
	})

	// Parse header first to get DataOffset
	headerData, err := v.fetchRange("any-txid", 0, 51)
	if err != nil {
		t.Fatalf("fetchRange header: %v", err)
	}
	v2Header, err := v.parseCarV2Header(headerData)
	if err != nil {
		t.Fatalf("parseCarV2Header: %v", err)
	}

	// 获取索引中的第一个条目
	indexData, err := v.fetchRange("any-txid", int64(v2Header.IndexOffset), 10000)
	if err != nil {
		t.Fatalf("fetchRange index: %v", err)
	}
	entries, err := v.parseIndex(indexData)
	if err != nil {
		t.Fatalf("parseIndex: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("no index entries")
	}

	// 验证第一个 block
	passed, err := v.verifyBlock("any-txid", v2Header, entries[0])
	if err != nil {
		t.Fatalf("verifyBlock failed: %v", err)
	}
	if !passed {
		t.Error("verifyBlock should pass")
	}
}

// TestVerifyBlock_TamperedData 测试篡改数据的 block 验证（应失败）
func TestVerifyBlock_TamperedData(t *testing.T) {
	mock := setupMockCARv2Server(t)
	defer mock.Close()

	gw := download.NewGateway(download.GatewayConfig{
		URLs:       []string{mock.URL()},
		Timeout:    5 * time.Second,
		MaxRetries: 1,
		RetryDelay: 10 * time.Millisecond,
	})

	v := NewOnlineVerifier(OnlineVerifierConfig{
		Gateway:        gw,
		SampleCount:    5,
		MaxConcurrency: 4,
	})

	headerData, err := v.fetchRange("any-txid", 0, 51)
	if err != nil {
		t.Fatalf("fetchRange header: %v", err)
	}
	v2Header, err := v.parseCarV2Header(headerData)
	if err != nil {
		t.Fatalf("parseCarV2Header: %v", err)
	}

	indexData, err := v.fetchRange("any-txid", int64(v2Header.IndexOffset), 10000)
	if err != nil {
		t.Fatalf("fetchRange index: %v", err)
	}
	entries, err := v.parseIndex(indexData)
	if err != nil {
		t.Fatalf("parseIndex: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("no index entries")
	}

	// 篡改：使用一个错误的 offset（指向其他数据位置）
	tamperedEntry := IndexEntry{
		MultihashDigest: entries[0].MultihashDigest,
		Offset:          entries[0].Offset + 1, // 偏移一个字节
	}

	passed, err := v.verifyBlock("any-txid", v2Header, tamperedEntry)
	if passed {
		t.Error("verifyBlock should fail with tampered offset")
	}
	// err may not be nil but the function returns passed=false
	_ = err
}

// TestForEachEntry 测试遍历所有索引条目
func TestForEachEntry(t *testing.T) {
	mock := setupMockCARv2Server(t)
	defer mock.Close()

	gw := download.NewGateway(download.GatewayConfig{
		URLs:       []string{mock.URL()},
		Timeout:    5 * time.Second,
		MaxRetries: 1,
		RetryDelay: 10 * time.Millisecond,
	})

	v := NewOnlineVerifier(OnlineVerifierConfig{
		Gateway:        gw,
		SampleCount:    5,
		MaxConcurrency: 4,
	})

	count := 0
	err := v.ForEachEntry("any-txid", func(e IndexEntry) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("ForEachEntry failed: %v", err)
	}
	if count != 20 {
		t.Errorf("ForEachEntry count = %d, want 20", count)
	}
}

// TestNewPipelineVerifier 测试 pipeline 集成
func TestNewPipelineVerifier(t *testing.T) {
	mock := setupMockCARv2Server(t)
	defer mock.Close()

	gw := download.NewGateway(download.GatewayConfig{
		URLs:       []string{mock.URL()},
		Timeout:    5 * time.Second,
		MaxRetries: 1,
		RetryDelay: 10 * time.Millisecond,
	})

	v := NewOnlineVerifier(OnlineVerifierConfig{
		Gateway:        gw,
		SampleCount:    5,
		MaxConcurrency: 4,
	})

	pv := NewPipelineVerifier(v, "any-txid")
	if pv == nil {
		t.Fatal("NewPipelineVerifier returned nil")
	}

	err := pv.VerifyIndex()
	if err != nil {
		t.Fatalf("VerifyIndex failed: %v", err)
	}
}

// TestIPFSBytesReader 测试字节读取器
func TestIPFSBytesReader(t *testing.T) {
	data := []byte{0x01, 0x02, 0x03, 0x04}
	br := newBytesReader(data)

	b, err := br.ReadByte()
	if err != nil {
		t.Fatalf("ReadByte: %v", err)
	}
	if b != 0x01 {
		t.Errorf("first byte = %x, want 0x01", b)
	}

	buf := make([]byte, 2)
	n, err := br.Read(buf)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if n != 2 || buf[0] != 0x02 || buf[1] != 0x03 {
		t.Errorf("Read got %d bytes: %x", n, buf[:n])
	}

	b, err = br.ReadByte()
	if err != nil {
		t.Fatalf("last ReadByte: %v", err)
	}
	if b != 0x04 {
		t.Errorf("last byte = %x, want 0x04", b)
	}
}

// TestFetchRange 测试 Range 请求（需要集成 gateway mock）
func TestFetchRange(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rangeHeader := r.Header.Get("Range")
		if rangeHeader == "" {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("full-content-here"))
			return
		}

		var start, end int64
		fmt.Sscanf(rangeHeader, "bytes=%d-%d", &start, &end)

		content := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
		if start >= 0 && end < int64(len(content)) {
			w.Header().Set("Content-Range",
				fmt.Sprintf("bytes %d-%d/%d", start, end, len(content)))
			w.WriteHeader(http.StatusPartialContent)
			w.Write(content[start : end+1])
		} else {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		}
	}))
	defer server.Close()

	gw := download.NewGateway(download.GatewayConfig{
		URLs:       []string{server.URL},
		Timeout:    5 * time.Second,
		MaxRetries: 1,
		RetryDelay: 10 * time.Millisecond,
	})

	data, err := gw.FetchRange("any-txid", 0, 10)
	if err != nil {
		t.Fatalf("FetchRange failed: %v", err)
	}
	if string(data) != "0123456789" {
		t.Errorf("FetchRange = %q, want %q", string(data), "0123456789")
	}

	data, err = gw.FetchRange("any-txid", 10, 16)
	if err != nil {
		t.Fatalf("FetchRange offset 10: %v", err)
	}
	if string(data) != "ABCDEFGHIJKLMNOP" {
		t.Errorf("FetchRange offset10 = %q", string(data))
	}
}

// ============================================================
// 并发测试
// ============================================================

func TestOnlineVerifier_Concurrency(t *testing.T) {
	mock := setupMockCARv2Server(t)
	defer mock.Close()

	gw := download.NewGateway(download.GatewayConfig{
		URLs:       []string{mock.URL()},
		Timeout:    5 * time.Second,
		MaxRetries: 1,
		RetryDelay: 10 * time.Millisecond,
	})

	// 限制并发数为 2
	v := NewOnlineVerifier(OnlineVerifierConfig{
		Gateway:        gw,
		SampleCount:    10,
		MaxConcurrency: 2,
	})

	// 确保信号量机制正常工作
	if cap(v.sema) != 2 {
		t.Errorf("sema capacity = %d, want 2", cap(v.sema))
	}

	result, err := v.Verify("any-txid")
	if err != nil {
		t.Fatalf("Verify failed: %v", err)
	}
	if !result.Passed {
		t.Error("Verify should pass")
	}
}

// ============================================================
// 边界情况
// ============================================================

func TestOnlineVerifier_EmptyDataTXID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	gw := download.NewGateway(download.GatewayConfig{
		URLs:       []string{server.URL},
		Timeout:    5 * time.Second,
		MaxRetries: 1,
		RetryDelay: 10 * time.Millisecond,
	})

	v := NewOnlineVerifier(OnlineVerifierConfig{
		Gateway:        gw,
		SampleCount:    5,
		MaxConcurrency: 4,
	})

	_, err := v.Verify("non-existent-txid")
	if err == nil {
		t.Error("Verify should fail for non-existent txid")
	}
}

func TestOnlineVerifier_ShortHeader(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("too-short"))
	}))
	defer server.Close()

	gw := download.NewGateway(download.GatewayConfig{
		URLs:       []string{server.URL},
		Timeout:    5 * time.Second,
		MaxRetries: 1,
		RetryDelay: 10 * time.Millisecond,
	})

	v := NewOnlineVerifier(OnlineVerifierConfig{
		Gateway:        gw,
		SampleCount:    5,
		MaxConcurrency: 4,
	})

	_, err := v.Verify("short-data-txid")
	if err == nil {
		t.Error("Verify should fail with short data")
	}
}

// ============================================================
// 格式测试
// ============================================================

func TestBuildMockIndexSorted(t *testing.T) {
	entries := []IndexEntry{
		{MultihashDigest: []byte{0xaa, 0xbb, 0xcc, 0xdd}, Offset: 42},
		{MultihashDigest: []byte{0x11, 0x22, 0x33, 0x44}, Offset: 84},
	}

	indexData := buildMockIndexSorted(entries)

	// 按宽度分组（所有 digest 长度相同）
	if len(indexData) < 10 {
		t.Errorf("index data too short: %d bytes", len(indexData))
	}
}

// ============================================================
// 辅助函数
// ============================================================

// 确保外部包被引用
var _ = ipfs.ValidateMultihash
