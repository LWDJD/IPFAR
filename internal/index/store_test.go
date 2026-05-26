// Package index 两层索引存储测试
package index

import (
	"fmt"
	"os"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

// newTestStore 创建用于测试的 Store 实例
func newTestStore(t *testing.T) (*Store, func()) {
	t.Helper()
	tmpDir, err := os.MkdirTemp("", "ipfar-index-test-*")
	if err != nil {
		t.Fatalf("创建临时目录失败: %v", err)
	}

	opts := badger.DefaultOptions(tmpDir)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("打开 Badger 失败: %v", err)
	}

	store := NewStore(db)
	cleanup := func() {
		store.Close()
		db.Close()
		os.RemoveAll(tmpDir)
	}
	return store, cleanup
}

// ============================================================
// 第一层：CID 索引测试
// ============================================================

func TestStore_IndexCID(t *testing.T) {
	s, cleanup := newTestStore(t)
	defer cleanup()

	cid := "bafkreiabcdef123456"
	metaTxID := "tx123456789"

	// 索引 CID
	if err := s.IndexCID(cid, metaTxID); err != nil {
		t.Fatalf("IndexCID 失败: %v", err)
	}

	// 验证 CID 存在
	has, err := s.HasCID(cid)
	if err != nil {
		t.Fatalf("HasCID 失败: %v", err)
	}
	if !has {
		t.Fatal("CID 应该存在")
	}

	// 获取 metaTxID 列表
	ids, err := s.GetCIDMetaIDs(cid)
	if err != nil {
		t.Fatalf("GetCIDMetaIDs 失败: %v", err)
	}
	if len(ids) != 1 || ids[0] != metaTxID {
		t.Fatalf("metaTxID 列表不正确: got %v", ids)
	}
}

func TestStore_IndexCID_Duplicate(t *testing.T) {
	s, cleanup := newTestStore(t)
	defer cleanup()

	cid := "bafkreiabcdef123456"
	metaTxID1 := "tx123456789"
	metaTxID2 := "tx987654321"

	// 索引同一个 CID 对应两个不同 metaTxID
	if err := s.IndexCID(cid, metaTxID1); err != nil {
		t.Fatalf("IndexCID 1 失败: %v", err)
	}
	if err := s.IndexCID(cid, metaTxID2); err != nil {
		t.Fatalf("IndexCID 2 失败: %v", err)
	}
	// 重复添加同一个 metaTxID（应被去重）
	if err := s.IndexCID(cid, metaTxID2); err != nil {
		t.Fatalf("IndexCID 3 (dup) 失败: %v", err)
	}

	ids, err := s.GetCIDMetaIDs(cid)
	if err != nil {
		t.Fatalf("GetCIDMetaIDs 失败: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("应有 2 个 metaTxID，实际 %d: %v", len(ids), ids)
	}
}

func TestStore_GetAllCIDs(t *testing.T) {
	s, cleanup := newTestStore(t)
	defer cleanup()

	cids := []string{"cid-a", "cid-b", "cid-c"}
	for i, cid := range cids {
		if err := s.IndexCID(cid, fmt.Sprintf("tx-%d", i)); err != nil {
			t.Fatalf("IndexCID %s 失败: %v", cid, err)
		}
	}

	all, err := s.GetAllCIDs()
	if err != nil {
		t.Fatalf("GetAllCIDs 失败: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("应有 3 个 CID，实际 %d: %v", len(all), all)
	}
}

// ============================================================
// 第二层：元数据索引测试
// ============================================================

func TestStore_IndexMeta(t *testing.T) {
	s, cleanup := newTestStore(t)
	defer cleanup()

	metaTxID := "meta-tx-123"
	params := map[string]string{
		"dataTxId":    "data-tx-456",
		"bundleTxId":  "none",
		"blockHeight": "1920000",
		"dataSize":    "1048576",
		"rootCid":     "bafkrei-test",
		"verified":    "none",
		"verifiedAt":  "0",
	}

	if err := s.IndexMeta(metaTxID, params); err != nil {
		t.Fatalf("IndexMeta 失败: %v", err)
	}

	// 验证能读回
	got, err := s.GetMeta(metaTxID)
	if err != nil {
		t.Fatalf("GetMeta 失败: %v", err)
	}
	if got == nil {
		t.Fatal("GetMeta 返回 nil")
	}
	if got["dataTxId"] != params["dataTxId"] {
		t.Fatalf("dataTxId 不匹配: got %q, want %q", got["dataTxId"], params["dataTxId"])
	}
	if got["blockHeight"] != params["blockHeight"] {
		t.Fatalf("blockHeight 不匹配: got %q, want %q", got["blockHeight"], params["blockHeight"])
	}
}

func TestStore_MarkVerified(t *testing.T) {
	s, cleanup := newTestStore(t)
	defer cleanup()

	metaTxID := "meta-tx-verified"

	// 先索引基础信息
	params := map[string]string{
		"dataTxId": "data-tx-1",
		"rootCid":  "bafkrei-1",
	}
	if err := s.IndexMeta(metaTxID, params); err != nil {
		t.Fatalf("IndexMeta 失败: %v", err)
	}

	// 标记为已验证
	if err := s.MarkVerified(metaTxID); err != nil {
		t.Fatalf("MarkVerified 失败: %v", err)
	}

	// 检查验证状态
	verified, err := s.IsVerified(metaTxID)
	if err != nil {
		t.Fatalf("IsVerified 失败: %v", err)
	}
	if !verified {
		t.Fatal("应该被标记为验证通过")
	}

	// 同时检查按步骤缓存
	for _, step := range []string{StepPoW, StepIndex, StepRefChain, StepIntegrity} {
		ok, err := s.IsStepVerified(metaTxID, step)
		if err != nil {
			t.Fatalf("IsStepVerified(%s) 失败: %v", step, err)
		}
		if !ok {
			t.Fatalf("步骤 %s 应该被标记为已验证", step)
		}
	}

	// 检查 AllStepsVerified
	allVerified, err := s.AllStepsVerified(metaTxID, []string{StepPoW, StepIndex, StepRefChain, StepIntegrity})
	if err != nil {
		t.Fatalf("AllStepsVerified 失败: %v", err)
	}
	if !allVerified {
		t.Fatal("AllStepsVerified 应该返回 true")
	}
}

func TestStore_GetAllMetaIDs(t *testing.T) {
	s, cleanup := newTestStore(t)
	defer cleanup()

	for i := 0; i < 5; i++ {
		metaTxID := fmt.Sprintf("meta-%d", i)
		params := map[string]string{"dataTxId": fmt.Sprintf("data-%d", i)}
		if err := s.IndexMeta(metaTxID, params); err != nil {
			t.Fatalf("IndexMeta %s 失败: %v", metaTxID, err)
		}
	}

	all, err := s.GetAllMetaIDs()
	if err != nil {
		t.Fatalf("GetAllMetaIDs 失败: %v", err)
	}
	if len(all) != 5 {
		t.Fatalf("应有 5 个 metaTxID，实际 %d", len(all))
	}
}

// ============================================================
// DHT 发布记录测试
// ============================================================

func TestStore_DHTRecord(t *testing.T) {
	s, cleanup := newTestStore(t)
	defer cleanup()

	cid := "bafkrei-dht-test"

	if err := s.MarkDHTPublished(cid); err != nil {
		t.Fatalf("MarkDHTPublished 失败: %v", err)
	}

	ts, err := s.GetDHTTimestamp(cid)
	if err != nil {
		t.Fatalf("GetDHTTimestamp 失败: %v", err)
	}
	if ts <= 0 {
		t.Fatal("时间戳应为正数")
	}
}

// ============================================================
// 向后兼容：旧版 Entry API 测试
// ============================================================

func TestStore_Put_BackwardCompatible(t *testing.T) {
	s, cleanup := newTestStore(t)
	defer cleanup()

	entry := &Entry{
		CID:        "bafkrei-old-entry",
		DataTXID:   "data-tx-999",
		BundleTXID: "",
		Height:     1920000,
		DataSize:   2048,
	}

	if err := s.Put(entry); err != nil {
		t.Fatalf("Put (backward compat) 失败: %v", err)
	}

	// 通过新 API 读取
	ids, err := s.GetCIDMetaIDs("bafkrei-old-entry")
	if err != nil {
		t.Fatalf("GetCIDMetaIDs 失败: %v", err)
	}
	if len(ids) != 1 || ids[0] != "data-tx-999" {
		t.Fatalf("CID 索引不匹配: got %v", ids)
	}

	params, err := s.GetMeta("data-tx-999")
	if err != nil {
		t.Fatalf("GetMeta 失败: %v", err)
	}
	if params["blockHeight"] != "1920000" {
		t.Fatalf("blockHeight 不匹配: got %q", params["blockHeight"])
	}

	// 通过旧 API 读取
	gotEntry, err := s.Get("bafkrei-old-entry")
	if err != nil {
		t.Fatalf("Get (backward compat) 失败: %v", err)
	}
	if gotEntry == nil {
		t.Fatal("Get 返回 nil")
	}
	if gotEntry.DataTXID != "data-tx-999" {
		t.Fatalf("DataTXID 不匹配: got %q", gotEntry.DataTXID)
	}
}

func TestStore_Has_BackwardCompatible(t *testing.T) {
	s, cleanup := newTestStore(t)
	defer cleanup()

	entry := &Entry{
		CID:      "bafkrei-has-test",
		DataTXID: "data-tx-has",
		Height:   100,
		DataSize: 500,
	}

	// 未索引前
	has, err := s.Has("bafkrei-has-test")
	if err != nil {
		t.Fatalf("Has 失败: %v", err)
	}
	if has {
		t.Fatal("未索引时 Has 应返回 false")
	}

	// 索引后
	if err := s.Put(entry); err != nil {
		t.Fatalf("Put 失败: %v", err)
	}

	has, err = s.Has("bafkrei-has-test")
	if err != nil {
		t.Fatalf("Has 失败: %v", err)
	}
	if !has {
		t.Fatal("索引后 Has 应返回 true")
	}
}

func TestStore_BatchPut_BackwardCompatible(t *testing.T) {
	s, cleanup := newTestStore(t)
	defer cleanup()

	entries := []*Entry{
		{CID: "cid-batch-1", DataTXID: "data-batch-1", Height: 10, DataSize: 100},
		{CID: "cid-batch-2", DataTXID: "data-batch-2", Height: 20, DataSize: 200},
		{CID: "cid-batch-3", DataTXID: "data-batch-3", Height: 30, DataSize: 300},
	}

	if err := s.BatchPut(entries); err != nil {
		t.Fatalf("BatchPut 失败: %v", err)
	}

	count, err := s.Count()
	if err != nil {
		t.Fatalf("Count 失败: %v", err)
	}
	if count != 3 {
		t.Fatalf("应有 3 条索引，实际 %d", count)
	}

	// 验证每个 CID 都可读
	for _, e := range entries {
		got, err := s.Get(e.CID)
		if err != nil {
			t.Fatalf("Get %s 失败: %v", e.CID, err)
		}
		if got == nil || got.DataTXID != e.DataTXID {
			t.Fatalf("CID %s 数据不匹配", e.CID)
		}
	}
}

func TestStore_Count(t *testing.T) {
	s, cleanup := newTestStore(t)
	defer cleanup()

	// 空 store
	count, err := s.Count()
	if err != nil {
		t.Fatalf("Count 失败: %v", err)
	}
	if count != 0 {
		t.Fatalf("空 store 应有 0 条，实际 %d", count)
	}

	// 添加一些
	for i := 0; i < 10; i++ {
		entry := &Entry{
			CID:      fmt.Sprintf("cid-count-%d", i),
			DataTXID: fmt.Sprintf("data-count-%d", i),
			Height:   int64(i * 100),
			DataSize: int64(i * 50),
		}
		if err := s.Put(entry); err != nil {
			t.Fatalf("Put %d 失败: %v", i, err)
		}
	}

	count, err = s.Count()
	if err != nil {
		t.Fatalf("Count 失败: %v", err)
	}
	if count != 10 {
		t.Fatalf("应有 10 条，实际 %d", count)
	}
}

func TestStore_Delete(t *testing.T) {
	s, cleanup := newTestStore(t)
	defer cleanup()

	cid := "cid-to-delete"
	entry := &Entry{CID: cid, DataTXID: "data-del", Height: 1, DataSize: 1}

	if err := s.Put(entry); err != nil {
		t.Fatalf("Put 失败: %v", err)
	}

	if err := s.Delete(cid); err != nil {
		t.Fatalf("Delete 失败: %v", err)
	}

	has, err := s.Has(cid)
	if err != nil {
		t.Fatalf("Has 失败: %v", err)
	}
	if has {
		t.Fatal("删除后 Has 应返回 false")
	}
}
