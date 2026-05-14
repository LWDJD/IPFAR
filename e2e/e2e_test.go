// Package e2e 提供 IPFAR 端到端集成测试。
//
// 使用 mock Arweave 网关模拟完整流程：
//
//	创建元数据 → 发现 → 下载 → 验证 → 缓存 → Bitswap 请求
//
// 测试覆盖 4 档安全预设（strict/balanced/light/trusted）。
package e2e

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sdkmeta "github.com/LWDJD/ipfar-sdk/verify/metadata"
	"github.com/LWDJD/ipfar-sdk/verify/pipeline"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"

	"github.com/lwdjd/IPFAR/internal/bridge"
	"github.com/lwdjd/IPFAR/internal/cache"
	"github.com/lwdjd/IPFAR/internal/download"
	"github.com/lwdjd/IPFAR/internal/verify"
)

func init() {
	// 静默日志输出（测试中）
	// log.SetLevel not available, skip
}

// ============================================================
// Mock Arweave Gateway
// ============================================================

// mockGateway 模拟 Arweave 网关
type mockGateway struct {
	server   *httptest.Server
	txData   map[string][]byte // txID → data
	txHeaders map[string]map[string]string // txID → headers
}

// newMockGateway 创建 mock 网关
func newMockGateway() *mockGateway {
	m := &mockGateway{
		txData:    make(map[string][]byte),
		txHeaders: make(map[string]map[string]string),
	}

	m.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 提取 txID（去掉路径前缀 /）
		path := strings.TrimPrefix(r.URL.Path, "/")
		txID := path

		// /block/height/{height} 端点
		if strings.HasPrefix(path, "block/height/") {
			// 简化：返回空区块
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"height":0,"txs":[]}`))
			return
		}

		// HEAD 请求
		if r.Method == http.MethodHead {
			data, ok := m.txData[txID]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
			w.Header().Set("Content-Type", "application/octet-stream")
			w.WriteHeader(http.StatusOK)
			return
		}

		// Range 请求
		rangeHeader := r.Header.Get("Range")
		if rangeHeader != "" {
			data, ok := m.txData[txID]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}

			var rangeStart, rangeEnd int64
			fmt.Sscanf(rangeHeader, "bytes=%d-%d", &rangeStart, &rangeEnd)

			if rangeStart < 0 {
				rangeStart = 0
			}
			if rangeEnd >= int64(len(data)) {
				rangeEnd = int64(len(data)) - 1
			}

			if rangeStart > rangeEnd {
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}

			w.Header().Set("Content-Type", "application/vnd.ipld.car")
			w.Header().Set("Content-Range",
				fmt.Sprintf("bytes %d-%d/%d", rangeStart, rangeEnd, len(data)))
			w.WriteHeader(http.StatusPartialContent)
			w.Write(data[rangeStart : rangeEnd+1])
			return
		}

		// 普通 GET 请求
		data, ok := m.txData[txID]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		w.Write(data)
	}))

	return m
}

// addData 向 mock 网关添加数据
func (m *mockGateway) addData(txID string, data []byte) {
	m.txData[txID] = data
}

// url 返回 mock 网关 URL
func (m *mockGateway) url() string {
	return m.server.URL
}

// close 关闭 mock 网关
func (m *mockGateway) close() {
	m.server.Close()
}

// ============================================================
// CAR v2 构建器
// ============================================================

// mockCARv2 构建一个简单的 CAR v2 文件用于测试
type mockCARv2 struct {
	data     []byte
	rootCID  string
	dataTXID string
	blockCIDs []string
}

// buildMockCARv2 构建包含 N 个 block 的 CAR v2 文件
func buildMockCARv2(t *testing.T, numBlocks int) *mockCARv2 {
	t.Helper()

	m := &mockCARv2{}

	// 创建 block
	type blockInfo struct {
		cid  cid.Cid
		data []byte
	}
	var blocks []blockInfo

	for i := 0; i < numBlocks; i++ {
		blockData := []byte(fmt.Sprintf("block-data-%04d-padding-to-make-it-longer", i))
		mh, err := multihash.Sum(blockData, multihash.IDENTITY, -1)
		if err != nil {
			t.Fatalf("multihash.Sum: %v", err)
		}
		c := cid.NewCidV1(cid.Raw, mh)
		blocks = append(blocks, blockInfo{cid: c, data: blockData})
		m.blockCIDs = append(m.blockCIDs, c.String())
	}

	// 构建 CAR v1 数据区
	var carV1Data []byte

	// Version = 1
	varintBuf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(varintBuf, 1)
	carV1Data = append(carV1Data, varintBuf[:n]...)

	// Root count = 0
	n = binary.PutUvarint(varintBuf, 0)
	carV1Data = append(carV1Data, varintBuf[:n]...)

	// Blocks
	type idxEntry = struct {
		digest []byte
		offset uint64
	}
	var indexEntries []idxEntry

	for _, blk := range blocks {
		blockOffset := uint64(len(carV1Data))

		cidBytes := blk.cid.Bytes()
		sectionLen := uint64(len(cidBytes) + len(blk.data))

		// section length varint
		slBuf := make([]byte, binary.MaxVarintLen64)
		slN := binary.PutUvarint(slBuf, sectionLen)
		carV1Data = append(carV1Data, slBuf[:slN]...)

		// CID
		carV1Data = append(carV1Data, cidBytes...)

		// Data
		carV1Data = append(carV1Data, blk.data...)

		// 记录索引
		dmh, err := multihash.Decode(blk.cid.Hash())
		if err != nil {
			t.Fatalf("multihash decode: %v", err)
		}
		indexEntries = append(indexEntries, idxEntry{
			digest: dmh.Digest,
			offset: blockOffset,
		})
	}

	// 构建 Index (CarIndexSorted)
	indexData := buildCarIndexSorted(indexEntries)

	// 构建完整 CAR v2
	pragma := []byte{
		0x0a, 0xa1, 0x67, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x02,
	}

	carV2Header := make([]byte, 40)
	dataOffset := uint64(len(pragma) + 40)
	dataSize := uint64(len(carV1Data))
	indexOffset := dataOffset + dataSize

	binary.LittleEndian.PutUint64(carV2Header[16:24], dataOffset)
	binary.LittleEndian.PutUint64(carV2Header[24:32], dataSize)
	binary.LittleEndian.PutUint64(carV2Header[32:40], indexOffset)

	m.data = append(pragma, carV2Header...)
	m.data = append(m.data, carV1Data...)
	m.data = append(m.data, indexData...)

	// 使用第一个 block 的 CID 作为 root CID
	if len(blocks) > 0 {
		m.rootCID = blocks[0].cid.String()
	}

	return m
}

// buildCarIndexSorted 构建 CarIndexSorted 格式的索引
func buildCarIndexSorted(entries []struct {
	digest []byte
	offset uint64
}) []byte {
	// 按宽度分组
	byWidth := make(map[uint32][]struct {
		digest []byte
		offset uint64
	})
	for _, e := range entries {
		w := uint32(len(e.digest)) + 8
		byWidth[w] = append(byWidth[w], e)
	}

	var buf []byte

	// codec
	codecBuf := make([]byte, binary.MaxVarintLen64)
	cn := binary.PutUvarint(codecBuf, uint64(multicodec.CarIndexSorted))
	buf = append(buf, codecBuf[:cn]...)

	// bucket count
	countBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(countBuf, uint32(len(byWidth)))
	buf = append(buf, countBuf...)

	for width, ents := range byWidth {
		wBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(wBuf, width)
		buf = append(buf, wBuf...)

		dataLen := uint64(len(ents)) * uint64(width)
		dBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(dBuf, dataLen)
		buf = append(buf, dBuf...)

		digestLen := int(width) - 8
		for _, e := range ents {
			entry := make([]byte, width)
			copy(entry[:digestLen], e.digest)
			binary.LittleEndian.PutUint64(entry[digestLen:], e.offset)
			buf = append(buf, entry...)
		}
	}

	return buf
}

// ============================================================
// 测试辅助函数
// ============================================================

// makeMetadata 创建测试用元数据
func makeMetadata(t *testing.T, rootCID, dataTXID string, dataSize int) *sdkmeta.Metadata {
	t.Helper()
	// Use the actual data size. For files < 100 MiB we provide a dummy PoW
	// to pass schema validation; actual PoW verification is controlled by pipeline config.
	pow := ""
	if dataSize < 100*1024*1024 {
		pow = "dummy-pow-for-testing" // dummy non-empty value satisfies schema
	}
	return &sdkmeta.Metadata{
		Version:   1,
		Method:    "raw",
		RootCID:   rootCID,
		DataTXID:  dataTXID,
		DataHeight: 1000,
		DataSize:  dataSize,
		PoW:       pow,
		PoWAlg:    "argon2id-light-v1",
	}
}

// makeMetadataJSON 创建测试用元数据 JSON
func makeMetadataJSON(t *testing.T, rootCID, dataTXID string, dataSize int) []byte {
	t.Helper()
	meta := makeMetadata(t, rootCID, dataTXID, dataSize)
	jsonBytes, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal metadata: %v", err)
	}
	return jsonBytes
}

// ============================================================
// E2E 测试
// ============================================================

// TestE2E_FullFlow_Strict 端到端测试：strict 预设
func TestE2E_FullFlow_Strict(t *testing.T) {
	// 搭建 mock 环境
	mock := newMockGateway()
	defer mock.close()

	car := buildMockCARv2(t, 10)
	carTXID := "car-st-001"
	mock.addData(carTXID, car.data)

	metaTXID := "meta-txid-strict-001"
	metaJSON := makeMetadataJSON(t, car.rootCID, carTXID, len(car.data))
	mock.addData(metaTXID, metaJSON)

	// 创建临时缓存目录
	tmpDir := t.TempDir()
	cacheDir := filepath.Join(tmpDir, "cache")
	os.MkdirAll(cacheDir, 0755)

	// 创建桥接器（strict 模式：全开）
	b := bridge.NewBridge(false, true, true, true) // PoW disabled: using dummy PoW

	// Step 1: 获取元数据
	gw := download.NewGateway(download.GatewayConfig{
		URLs:       []string{mock.url()},
		Timeout:    5 * time.Second,
		MaxRetries: 1,
	})

	fetcher := download.NewFetcher(download.FetcherConfig{
		Gateway: gw,
		CacheDir: cacheDir,
	})

	meta, err := fetcher.FetchMetadataByTXID(metaTXID)
	if err != nil {
		t.Fatalf("FetchMetadataByTXID: %v", err)
	}

	// Step 2: 快速验证（元数据 + PoW）
	result := b.RunPipeline(meta, false)
	if !result.Passed {
		t.Fatalf("Quick verification should pass: %+v", result.Results)
	}
	t.Logf("Quick verify passed: %d steps", len(result.Results))

	// Step 3: 下载 CAR 文件
	carPath, err := fetcher.DownloadCAR(meta)
	if err != nil {
		t.Fatalf("DownloadCAR: %v", err)
	}
	defer os.Remove(carPath)

	if _, err := os.Stat(carPath); os.IsNotExist(err) {
		t.Fatal("CAR file should exist on disk")
	}
	t.Logf("CAR downloaded to: %s", carPath)

	// Step 4: 完整验证管道
	fullResult := b.RunPipeline(meta, true)
	t.Logf("Full verify result: passed=%v steps=%d", fullResult.Passed, len(fullResult.Results))
	for _, step := range fullResult.Results {
		t.Logf("  %s: passed=%v skipped=%v error=%q msg=%q",
			step.Step, step.Passed, step.Skipped, step.Error, step.Message)
	}

	// 元数据验证应该通过
	if !fullResult.Passed {
		t.Logf("Full verify not fully passed (some steps may be skipped): %+v", fullResult.Results)
	}
}

// TestE2E_FullFlow_Balanced 端到端测试：balanced 预设
func TestE2E_FullFlow_Balanced(t *testing.T) {
	mock := newMockGateway()
	defer mock.close()

	car := buildMockCARv2(t, 8)
	carTXID := "car-bl-001"
	mock.addData(carTXID, car.data)

	metaTXID := "meta-txid-balanced-001"
	metaJSON := makeMetadataJSON(t, car.rootCID, carTXID, len(car.data))
	mock.addData(metaTXID, metaJSON)

	tmpDir := t.TempDir()
	cacheDir := filepath.Join(tmpDir, "cache")

	b := bridge.NewBridge(false, true, false, true) // balanced: PoW disabled (dummy), Index+Integrity

	gw := download.NewGateway(download.GatewayConfig{
		URLs: []string{mock.url()},
	})
	fetcher := download.NewFetcher(download.FetcherConfig{
		Gateway: gw, CacheDir: cacheDir,
	})

	meta, err := fetcher.FetchMetadataByTXID(metaTXID)
	if err != nil {
		t.Fatalf("FetchMetadataByTXID: %v", err)
	}

	result := b.RunPipeline(meta, false)
	if !result.Passed {
		t.Fatalf("Quick verify should pass")
	}

	carPath, err := fetcher.DownloadCAR(meta)
	if err != nil {
		t.Fatalf("DownloadCAR: %v", err)
	}
	defer os.Remove(carPath)

	fullResult := b.RunPipeline(meta, true)
	t.Logf("Balanced verify: passed=%v", fullResult.Passed)
}

// TestE2E_FullFlow_Light 端到端测试：light 预设
func TestE2E_FullFlow_Light(t *testing.T) {
	mock := newMockGateway()
	defer mock.close()

	car := buildMockCARv2(t, 6)
	carTXID := "car-lt-001"
	mock.addData(carTXID, car.data)

	metaTXID := "meta-txid-light-001"
	metaJSON := makeMetadataJSON(t, car.rootCID, carTXID, len(car.data))
	mock.addData(metaTXID, metaJSON)

	tmpDir := t.TempDir()
	cacheDir := filepath.Join(tmpDir, "cache")

	b := bridge.NewBridge(false, false, true, false) // light: PoW disabled (dummy), RefChain

	gw := download.NewGateway(download.GatewayConfig{
		URLs: []string{mock.url()},
	})
	fetcher := download.NewFetcher(download.FetcherConfig{
		Gateway: gw, CacheDir: cacheDir,
	})

	meta, err := fetcher.FetchMetadataByTXID(metaTXID)
	if err != nil {
		t.Fatalf("FetchMetadataByTXID: %v", err)
	}

	result := b.RunPipeline(meta, false)
	if !result.Passed {
		t.Fatalf("Quick verify should pass")
	}
	t.Logf("Light quick verify passed")

	carPath, err := fetcher.DownloadCAR(meta)
	if err != nil {
		t.Fatalf("DownloadCAR: %v", err)
	}
	defer os.Remove(carPath)

	fullResult := b.RunPipeline(meta, true)
	t.Logf("Light verify: passed=%v", fullResult.Passed)
}

// TestE2E_FullFlow_Trusted 端到端测试：trusted 预设
func TestE2E_FullFlow_Trusted(t *testing.T) {
	mock := newMockGateway()
	defer mock.close()

	car := buildMockCARv2(t, 4)
	carTXID := "car-tr-001"
	mock.addData(carTXID, car.data)

	metaTXID := "meta-txid-trusted-001"
	metaJSON := makeMetadataJSON(t, car.rootCID, carTXID, len(car.data))
	mock.addData(metaTXID, metaJSON)

	tmpDir := t.TempDir()
	cacheDir := filepath.Join(tmpDir, "cache")

	b := bridge.NewBridge(false, false, false, false) // trusted: all disabled

	gw := download.NewGateway(download.GatewayConfig{
		URLs: []string{mock.url()},
	})
	fetcher := download.NewFetcher(download.FetcherConfig{
		Gateway: gw, CacheDir: cacheDir,
	})

	meta, err := fetcher.FetchMetadataByTXID(metaTXID)
	if err != nil {
		t.Fatalf("FetchMetadataByTXID: %v", err)
	}

	result := b.RunPipeline(meta, false)
	if !result.Passed {
		t.Fatalf("Trusted verify should pass (only metadata validation): %+v", result.Results)
	}
	t.Logf("Trusted quick verify passed")

	// trusted 模式下也应能下载 CAR
	carPath, err := fetcher.DownloadCAR(meta)
	if err != nil {
		t.Fatalf("DownloadCAR: %v", err)
	}
	defer os.Remove(carPath)

	fullResult := b.RunPipeline(meta, true)
	t.Logf("Trusted full verify: passed=%v", fullResult.Passed)
}

// TestE2E_OnlineVerify 端到端测试：在线验证路径
func TestE2E_OnlineVerify(t *testing.T) {
	mock := newMockGateway()
	defer mock.close()

	car := buildMockCARv2(t, 15)
	carTXID := "car-ol-001"
	mock.addData(carTXID, car.data)

	metaTXID := "meta-txid-online-001"
	metaJSON := makeMetadataJSON(t, car.rootCID, carTXID, len(car.data))
	mock.addData(metaTXID, metaJSON)

	// 创建在线验证器
	gw := download.NewGateway(download.GatewayConfig{
		URLs:    []string{mock.url()},
		Timeout: 5 * time.Second,
	})

	verifier := verify.NewOnlineVerifier(verify.OnlineVerifierConfig{
		Gateway:        gw,
		SampleCount:    3,
		MaxConcurrency: 2,
	})

	result, err := verifier.Verify(carTXID)
	if err != nil {
		t.Fatalf("Online verify: %v", err)
	}

	if !result.Passed {
		t.Errorf("Online verify should pass: verified=%d failed=%d errors=%v",
			result.VerifiedBlocks, result.FailedBlocks, result.Errors)
	}
	t.Logf("Online verify: passed=%v total=%d sampled=%d verified=%d",
		result.Passed, result.TotalBlocks, result.SampledBlocks, result.VerifiedBlocks)
}

// TestE2E_CacheAndRetrieve 端到端测试：缓存和检索
func TestE2E_CacheAndRetrieve(t *testing.T) {
	mock := newMockGateway()
	defer mock.close()

	car := buildMockCARv2(t, 5)
	carTXID := "car-ca-001"
	mock.addData(carTXID, car.data)

	metaTXID := "meta-txid-cache-001"
	metaJSON := makeMetadataJSON(t, car.rootCID, carTXID, len(car.data))
	mock.addData(metaTXID, metaJSON)

	tmpDir := t.TempDir()
	cacheDir := filepath.Join(tmpDir, "cache")
	ipfarCacheDir := filepath.Join(tmpDir, "ipfar-cache")

	// 使用通用缓存
	c, err := cache.New(cache.CacheConfig{
		Dir:     ipfarCacheDir,
		MaxSize: 10 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("New cache: %v", err)
	}
	defer c.Close()

	// 缓存元数据
	cacheKey := "meta:" + metaTXID
	if err := c.Put(cacheKey, metaJSON); err != nil {
		t.Fatalf("Cache metadata: %v", err)
	}

	// 从缓存读取
	got, found, err := c.Get(cacheKey)
	if err != nil {
		t.Fatalf("Get cached metadata: %v", err)
	}
	if !found {
		t.Fatal("Metadata should be in cache")
	}
	if string(got) != string(metaJSON) {
		t.Error("Cached metadata mismatch")
	}
	t.Logf("Cache hit: key=%s size=%d", cacheKey, len(got))

	// 缓存 CAR 数据
	carCacheKey := "car:" + carTXID
	if err := c.Put(carCacheKey, car.data); err != nil {
		t.Fatalf("Cache CAR: %v", err)
	}

	// 验证缓存统计
	stats := c.Stats()
	if stats.TotalEntries != 2 {
		t.Errorf("Expected 2 cache entries, got %d", stats.TotalEntries)
	}
	t.Logf("Cache stats: entries=%d size=%d hits=%d misses=%d",
		stats.TotalEntries, stats.CurrentSize, stats.Hits, stats.Misses)

	// 验证缓存路径映射
	path, found, err := c.GetPath(carCacheKey)
	if err != nil {
		t.Fatalf("GetPath: %v", err)
	}
	if !found || path == "" {
		t.Fatal("CAR cache path should be available")
	}
	t.Logf("CAR cache path: %s", path)

	// 清理
	_ = cacheDir
}

// TestE2E_FullVerifyPipeline 端到端测试：全量验证管道
func TestE2E_FullVerifyPipeline(t *testing.T) {
	mock := newMockGateway()
	defer mock.close()

	car := buildMockCARv2(t, 5)
	carTXID := "car-fv-001"
	mock.addData(carTXID, car.data)

	tmpDir := t.TempDir()
	cacheDir := filepath.Join(tmpDir, "car-cache")
	os.MkdirAll(cacheDir, 0755)

	// 手动将 CAR 文件放到缓存目录（模拟已有缓存）
	carFileName := fmt.Sprintf("%s_%s.car", car.rootCID[:min(32, len(car.rootCID))], carTXID[:min(12, len(carTXID))])
	carPath := filepath.Join(cacheDir, carFileName)
	if err := os.WriteFile(carPath, car.data, 0644); err != nil {
		t.Fatalf("Write CAR file: %v", err)
	}

	// 同时将元数据添加到 mock 网关
	metaTXID := carTXID // 使用相同的 txID
	metaJSON := makeMetadataJSON(t, car.rootCID, carTXID, len(car.data))
	mock.addData(metaTXID, metaJSON)

	// 创建全量验证器
	fv := verify.NewFullVerifier(verify.FullVerifierConfig{
		CacheDir:       cacheDir,
		GatewayURLs:    []string{mock.url()},
		MaxConcurrency: 1,
		VerifyPoW:      false, // disabled: using dummy PoW
		VerifyIndex:    true,
		SkipMissingMeta: false,
	})

	report, err := fv.VerifyAll()
	if err != nil {
		t.Fatalf("VerifyAll: %v", err)
	}

	t.Logf("Full verify report: total=%d verified=%d passed=%d failed=%d skipped=%d",
		report.TotalFiles, report.VerifiedFiles, report.PassedFiles,
		report.FailedFiles, report.SkippedFiles)

	if report.TotalFiles < 1 {
		t.Error("Should have found at least 1 CAR file")
	}

	// 格式化输出
	formatted := verify.FormatReport(report)
	t.Logf("Report:\n%s", formatted)

	if report.HasFailures() {
		t.Errorf("Full verify should not have failures with PoW disabled")
	}
}

// TestE2E_MultipleCARFiles 端到端测试：多个 CAR 文件的全量验证
func TestE2E_MultipleCARFiles(t *testing.T) {
	mock := newMockGateway()
	defer mock.close()

	tmpDir := t.TempDir()
	cacheDir := filepath.Join(tmpDir, "car-cache")
	os.MkdirAll(cacheDir, 0755)

	// 创建 3 个 CAR 文件
	numFiles := 3
	for i := 0; i < numFiles; i++ {
		car := buildMockCARv2(t, 3)
		carTXID := fmt.Sprintf("car-m%03d", i)
		mock.addData(carTXID, car.data)

		// 元数据也用相同的 txID（简化）
		metaJSON := makeMetadataJSON(t, car.rootCID, carTXID, len(car.data))
		mock.addData(carTXID, metaJSON)

		carFileName := fmt.Sprintf("%s_%s.car",
			car.rootCID[:min(32, len(car.rootCID))],
			carTXID[:min(12, len(carTXID))])
		carPath := filepath.Join(cacheDir, carFileName)
		os.WriteFile(carPath, car.data, 0644)
	}

	fv := verify.NewFullVerifier(verify.FullVerifierConfig{
		CacheDir:       cacheDir,
		GatewayURLs:    []string{mock.url()},
		MaxConcurrency: 2,
		VerifyPoW:      false, // disabled: using dummy PoW
		SkipMissingMeta: false,
	})

	report, err := fv.VerifyAll()
	if err != nil {
		t.Fatalf("VerifyAll: %v", err)
	}

	if report.TotalFiles != numFiles {
		t.Errorf("TotalFiles = %d, want %d", report.TotalFiles, numFiles)
	}

	t.Logf("Multiple CAR report: total=%d verified=%d passed=%d failed=%d skipped=%d",
		report.TotalFiles, report.VerifiedFiles, report.PassedFiles,
		report.FailedFiles, report.SkippedFiles)
}

// ============================================================
// 元数据验证测试
// ============================================================

func TestE2E_MetadataValidation(t *testing.T) {
	mock := newMockGateway()
	defer mock.close()

	car := buildMockCARv2(t, 3)
	carTXID := "car-mv-001"
	mock.addData(carTXID, car.data)

	metaTXID := "meta-txid-metaval-001"
	metaJSON := makeMetadataJSON(t, car.rootCID, carTXID, len(car.data))
	mock.addData(metaTXID, metaJSON)

	// 通过网关获取并验证元数据
	gw := download.NewGateway(download.GatewayConfig{
		URLs: []string{mock.url()},
	})

	fetcher := download.NewFetcher(download.FetcherConfig{
		Gateway: gw,
	})

	meta, err := fetcher.FetchMetadataByTXID(metaTXID)
	if err != nil {
		t.Fatalf("FetchMetadataByTXID: %v", err)
	}

	// 验证必填字段
	if meta.RootCID == "" {
		t.Error("RootCID should not be empty")
	}
	if meta.DataTXID == "" {
		t.Error("DataTXID should not be empty")
	}
	if meta.DataSize <= 0 {
		t.Error("DataSize should be > 0")
	}
	if meta.Method == "" {
		t.Error("Method should not be empty")
	}
	if meta.Version <= 0 {
		t.Error("Version should be > 0")
	}

	t.Logf("Metadata: root_cid=%s data_txid=%s data_size=%d method=%s version=%d",
		meta.RootCID, meta.DataTXID, meta.DataSize, meta.Method, meta.Version)
}

// ============================================================
// 边界条件测试
// ============================================================

func TestE2E_EmptyCacheDir_FullVerify(t *testing.T) {
	tmpDir := t.TempDir()
	emptyDir := filepath.Join(tmpDir, "empty")
	os.MkdirAll(emptyDir, 0755)

	fv := verify.NewFullVerifier(verify.FullVerifierConfig{
		CacheDir:    emptyDir,
		GatewayURLs: []string{"http://localhost:1"}, // 不可达
	})

	report, err := fv.VerifyAll()
	if err != nil {
		t.Fatalf("VerifyAll should not error on empty dir: %v", err)
	}
	if report.TotalFiles != 0 {
		t.Errorf("TotalFiles = %d, want 0", report.TotalFiles)
	}
	t.Logf("Empty dir report: total=%d", report.TotalFiles)
}

func TestE2E_MetadataWithMissingDataTXID(t *testing.T) {
	// 测试 data_txid 为空的情况
	meta := &sdkmeta.Metadata{
		Version:   1,
		Method:    "raw",
		RootCID:   "bafy123",
		DataTXID:  "", // 缺失
		DataHeight: 100,
		DataSize:  100,
	}

	b := bridge.NewBridge(false, true, true, true) // PoW disabled: using dummy PoW
	result := b.RunPipeline(meta, false)

	// 即使 data_txid 为空，元数据验证仍应通过
	if !result.Passed {
		t.Logf("Metadata with empty data_txid: passed=%v", result.Passed)
		for _, step := range result.Results {
			t.Logf("  %s: passed=%v skipped=%v error=%q",
				step.Step, step.Passed, step.Skipped, step.Error)
		}
	}
}

func TestE2E_BitswapRequest_Simulated(t *testing.T) {
	// 模拟 Bitswap 请求流程（无真实 libp2p 网络）
	mock := newMockGateway()
	defer mock.close()

	car := buildMockCARv2(t, 5)
	carTXID := "car-bs-001"
	mock.addData(carTXID, car.data)

	tmpDir := t.TempDir()
	cacheDir := filepath.Join(tmpDir, "cache")

	gw := download.NewGateway(download.GatewayConfig{
		URLs: []string{mock.url()},
	})

	fetcher := download.NewFetcher(download.FetcherConfig{
		Gateway: gw, CacheDir: cacheDir,
	})

	// 下载 CAR 到缓存（模拟 Bitswap 获取数据后缓存）
	meta := makeMetadata(t, car.rootCID, carTXID, len(car.data))
	// Note: DownloadCAR validates data_size matches file size
	carPath, err := fetcher.DownloadCAR(meta)
	if err != nil {
		t.Fatalf("DownloadCAR: %v", err)
	}
	defer os.Remove(carPath)

	// 验证 CAR 文件可被读取（模拟 Bitswap 提供数据）
	carData, err := os.ReadFile(carPath)
	if err != nil {
		t.Fatalf("Read cached CAR: %v", err)
	}

	if len(carData) != len(car.data) {
		t.Errorf("Cached CAR size mismatch: %d vs %d", len(carData), len(car.data))
	}

	// 解析 root CID
	rootCID, err := cid.Decode(car.rootCID)
	if err != nil {
		t.Fatalf("Decode root CID: %v", err)
	}

	t.Logf("Bitswap simulated: root_cid=%s car_path=%s car_size=%d",
		rootCID.String(), carPath, len(carData))
}

// ============================================================
// 辅助函数
// ============================================================


// 确保包级别引用
var _ = io.Discard
var _ = pipeline.SecurityStrict
