// Package index 提供两层索引架构：
//
// 第一层：CID 索引  HashMap<CID, HashSet<metaTxID>>
//   - Key: IPFS CID (string)
//   - Value: 该 CID 对应的所有元数据交易 ID 集合
//   - 用途：查重、DHT 发布内容列表
//
// 第二层：元数据索引  HashMap<metaTxID, HashMap<string, string>>
//   - Key: Arweave 元数据交易 ID
//   - Value: 参数键值对（dataTxId, bundleTxId, blockHeight, dataSize, rootCid, verified, verifiedAt）
//   - 用途：Bitswap 收到请求时快速定位数据位置
//
// 存储基于 Badger，键格式：
//   - CID 索引：  "cid:" + cid  → JSON 序列化的 []string（metaTxID 列表）
//   - 元数据索引："meta:" + metaTxID → JSON 序列化的 map[string]string
//   - DHT 发布记录："dht:" + cid → 时间戳
//
// 线程安全：Badger 原生支持并发读写。
package index

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/dgraph-io/badger/v4"

	"github.com/lwdjd/IPFAR/internal/log"
)

// 验证步骤常量（与 SDK pipeline 步骤名称保持一致）
const (
	StepPoW          = "pow"
	StepIndex        = "index"
	StepRefChain     = "reference_chain"
	StepIntegrity    = "integrity"
	StepMetaValidate = "meta_validate" // 强制，但也可以缓存
)

// 安全级别常量
const (
	LevelStrict   = "strict"
	LevelBalanced = "balanced"
	LevelLight    = "light"
	LevelTrusted  = "trusted"
)

// 键前缀常量
const (
	prefixCID      = "cid:" // CID 索引键前缀
	prefixMeta     = "meta:" // 元数据索引键前缀
	prefixDHT      = "dht:"  // DHT 发布记录键前缀
	prefixVerified = "v:"    // 按步骤验证记录键前缀
)

// Store 两层索引存储
// 线程安全（Badger 原生支持并发读写）
type Store struct {
	db *badger.DB
}

// NewStore 创建新的索引存储
// db 为已打开的 Badger 实例（复用）
func NewStore(db *badger.DB) *Store {
	return &Store{db: db}
}

// ============================================================
// 第一层：CID 索引
// ============================================================

// IndexCID 索引一个 CID→metaTxID 的映射
// 如果 CID 已有该 metaTxID，则跳过（去重）
func (s *Store) IndexCID(cid, metaTxID string) error {
	if cid == "" {
		return fmt.Errorf("index: CID must not be empty")
	}
	if metaTxID == "" {
		return fmt.Errorf("index: metaTxID must not be empty")
	}

	key := []byte(prefixCID + cid)

	return s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		var existing []string
		if err == nil {
			// 键存在，反序列化已有列表
			err = item.Value(func(v []byte) error {
				return json.Unmarshal(v, &existing)
			})
			if err != nil {
				// 数据损坏，重新开始
				existing = nil
			}
		} else if err != badger.ErrKeyNotFound {
			return fmt.Errorf("index: get cid %s: %w", cid, err)
		}

		// 去重检查
		for _, id := range existing {
			if id == metaTxID {
				return nil // 已存在，跳过
			}
		}

		// 追加
		existing = append(existing, metaTxID)
		newVal, err := json.Marshal(existing)
		if err != nil {
			return fmt.Errorf("index: marshal cid list: %w", err)
		}
		return txn.Set(key, newVal)
	})
}

// GetCIDMetaIDs 获取一个 CID 的所有元数据 ID
func (s *Store) GetCIDMetaIDs(cid string) ([]string, error) {
	if cid == "" {
		return nil, nil
	}

	key := []byte(prefixCID + cid)
	var result []string

	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return err
		}
		return item.Value(func(v []byte) error {
			return json.Unmarshal(v, &result)
		})
	})
	if err != nil {
		return nil, fmt.Errorf("index: get cid meta ids %s: %w", cid, err)
	}
	return result, nil
}

// GetAllCIDs 获取所有已索引的 CID（用于 DHT 发布）
func (s *Store) GetAllCIDs() ([]string, error) {
	var cids []string

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(prefixCID)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := string(item.Key())
			cid := key[len(prefixCID):]
			cids = append(cids, cid)
		}
		return nil
	})
	return cids, err
}

// HasCID 检查 CID 是否已存在
func (s *Store) HasCID(cid string) (bool, error) {
	if cid == "" {
		return false, nil
	}

	key := []byte(prefixCID + cid)
	found := false
	err := s.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return err
		}
		found = true
		return nil
	})
	return found, err
}

// DeleteCID 删除一条 CID 索引
func (s *Store) DeleteCID(cid string) error {
	if cid == "" {
		return nil
	}
	key := []byte(prefixCID + cid)
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

// ============================================================
// 第二层：元数据索引
// ============================================================

// IndexMeta 索引一个元数据交易的详细信息
// params 包含: dataTxId, bundleTxId, blockHeight, dataSize, rootCid, verified, verifiedAt
func (s *Store) IndexMeta(metaTxID string, params map[string]string) error {
	if metaTxID == "" {
		return fmt.Errorf("index: metaTxID must not be empty")
	}
	if params == nil {
		return fmt.Errorf("index: params must not be nil")
	}

	// 合并已有参数（如果存在）
	existing, err := s.GetMeta(metaTxID)
	if err != nil {
		return fmt.Errorf("index: get existing meta %s: %w", metaTxID, err)
	}
	if existing != nil {
		for k, v := range params {
			existing[k] = v
		}
		params = existing
	}

	val, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("index: marshal meta params: %w", err)
	}

	key := []byte(prefixMeta + metaTxID)
	err = s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, val)
	})
	if err != nil {
		return fmt.Errorf("index: put meta %s: %w", metaTxID, err)
	}

	log.Debug("索引：元数据写入 metaTxID=%s params=%v", metaTxID, params)
	return nil
}

// GetMeta 获取元数据交易的详细信息
func (s *Store) GetMeta(metaTxID string) (map[string]string, error) {
	if metaTxID == "" {
		return nil, nil
	}

	key := []byte(prefixMeta + metaTxID)
	var params map[string]string

	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return err
		}
		return item.Value(func(v []byte) error {
			return json.Unmarshal(v, &params)
		})
	})
	if err != nil {
		return nil, fmt.Errorf("index: get meta %s: %w", metaTxID, err)
	}
	return params, nil
}

// ============================================================
// 按步骤验证缓存
// ============================================================

// （MarkVerified / IsVerified 便捷方法在按步骤方法之后定义，见下方）

// buildStepKey 构建按步骤验证的 Badger 键
// 格式: "v:" + level + ":" + metaTxID + ":" + step
func buildStepKey(level, metaTxID, step string) string {
	return prefixVerified + level + ":" + metaTxID + ":" + step
}

// MarkStepVerifiedAtLevel 在特定安全级别下标记某步骤已验证通过
func (s *Store) MarkStepVerifiedAtLevel(metaTxID, step, level string) error {
	if metaTxID == "" {
		return fmt.Errorf("index: metaTxID must not be empty")
	}
	if step == "" {
		return fmt.Errorf("index: step must not be empty")
	}
	if level == "" {
		level = LevelLight
	}

	key := []byte(buildStepKey(level, metaTxID, step))
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, []byte("true"))
	})
}

// IsStepVerifiedAtLevel 检查特定级别下某步骤是否已验证通过
func (s *Store) IsStepVerifiedAtLevel(metaTxID, step, level string) (bool, error) {
	if metaTxID == "" || step == "" {
		return false, nil
	}
	if level == "" {
		level = LevelLight
	}

	key := []byte(buildStepKey(level, metaTxID, step))
	found := false
	err := s.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return err
		}
		found = true
		return nil
	})
	return found, err
}

// MarkStepVerified 标记某一步骤已验证通过（默认 light 级别）
func (s *Store) MarkStepVerified(metaTxID, step string) error {
	return s.MarkStepVerifiedAtLevel(metaTxID, step, LevelLight)
}

// IsStepVerified 检查某一步骤是否已验证通过（默认 light 级别）
func (s *Store) IsStepVerified(metaTxID, step string) (bool, error) {
	return s.IsStepVerifiedAtLevel(metaTxID, step, LevelLight)
}

// GetVerifiedSteps 获取某元数据所有已验证通过的步骤列表（所有级别）
func (s *Store) GetVerifiedSteps(metaTxID string) ([]string, error) {
	if metaTxID == "" {
		return nil, nil
	}

	var steps []string
	seen := make(map[string]bool)

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(prefixVerified)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := string(item.Key())
			// 键格式: "v:level:metaTxID:step"
			// 去掉前缀 "v:" 后按 ":" 分割
			rest := key[len(prefixVerified):]
			parts := splitN(rest, ":", 3)
			if len(parts) != 3 {
				continue
			}
			// parts[0]=level, parts[1]=metaTxID, parts[2]=step
			if parts[1] == metaTxID {
				if !seen[parts[2]] {
					steps = append(steps, parts[2])
					seen[parts[2]] = true
				}
			}
		}
		return nil
	})
	return steps, err
}

// splitN 分割字符串为最多 n 部分
func splitN(s, sep string, n int) []string {
	result := make([]string, 0, n)
	for i := 0; i < n-1; i++ {
		idx := indexOf(s, sep)
		if idx < 0 {
			result = append(result, s)
			return result
		}
		result = append(result, s[:idx])
		s = s[idx+len(sep):]
	}
	if s != "" || len(result) > 0 {
		result = append(result, s)
	}
	return result
}

// indexOf 返回 sep 在 s 中第一次出现的位置，-1 表示未找到
func indexOf(s, sep string) int {
	for i := 0; i <= len(s)-len(sep); i++ {
		if s[i:i+len(sep)] == sep {
			return i
		}
	}
	return -1
}

// MarkVerified 标记某个 metaTxID 已通过某级别验证（便捷方法）
// 内部将标记所有主要步骤（pow, index, ref_chain, integrity）为已验证
func (s *Store) MarkVerified(metaTxID, level string) error {
	if metaTxID == "" {
		return fmt.Errorf("index: metaTxID must not be empty")
	}
	if level == "" {
		level = LevelLight
	}

	// 同时更新元数据索引中的 verified 字段（保持向后兼容）
	existing, err := s.GetMeta(metaTxID)
	if err != nil {
		return fmt.Errorf("index: get meta before mark: %w", err)
	}
	if existing == nil {
		existing = make(map[string]string)
	}
	existing["verified"] = level
	existing["verifiedAt"] = strconv.FormatInt(time.Now().Unix(), 10)
	if err := s.IndexMeta(metaTxID, existing); err != nil {
		return fmt.Errorf("index: update meta verified: %w", err)
	}

	// 标记所有主要步骤
	for _, step := range []string{StepPoW, StepIndex, StepRefChain, StepIntegrity} {
		if err := s.MarkStepVerifiedAtLevel(metaTxID, step, level); err != nil {
			return fmt.Errorf("index: mark step %s verified: %w", step, err)
		}
	}
	return nil
}

// IsVerified 检查某个 metaTxID 是否已验证过（便捷方法）
// level 为空时，检查是否通过任意级别验证
// 新实现：同时检查旧的 meta 字段和新的按步骤缓存
func (s *Store) IsVerified(metaTxID, level string) (bool, error) {
	if metaTxID == "" {
		return false, nil
	}

	// 先检查旧的 meta 字段（向后兼容）
	params, err := s.GetMeta(metaTxID)
	if err != nil {
		return false, err
	}
	if params != nil {
		verifiedLevel, ok := params["verified"]
		if ok && verifiedLevel != "none" && verifiedLevel != "" {
			if level == "" {
				return true, nil
			}
			if verifiedLevel == level {
				return true, nil
			}
		}
	}

	// 再检查按步骤缓存
	if level == "" {
		// 检查任意级别的任意步骤
		steps, err := s.GetVerifiedSteps(metaTxID)
		if err != nil {
			return false, err
		}
		return len(steps) > 0, nil
	}

	// 检查指定级别的所有主要步骤是否都已通过
	for _, step := range []string{StepPoW, StepIndex, StepRefChain, StepIntegrity} {
		ok, err := s.IsStepVerifiedAtLevel(metaTxID, step, level)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

// GetAllMetaIDs 获取所有元数据 ID（用于启动时恢复）
func (s *Store) GetAllMetaIDs() ([]string, error) {
	var ids []string

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(prefixMeta)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := string(item.Key())
			id := key[len(prefixMeta):]
			ids = append(ids, id)
		}
		return nil
	})
	return ids, err
}

// ============================================================
// DHT 发布记录
// ============================================================

// MarkDHTPublished 记录一个 CID 已被 DHT 发布
func (s *Store) MarkDHTPublished(cid string) error {
	if cid == "" {
		return nil
	}
	key := []byte(prefixDHT + cid)
	ts := []byte(strconv.FormatInt(time.Now().Unix(), 10))
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, ts)
	})
}

// GetDHTTimestamp 获取 CID 的 DHT 发布时间戳
func (s *Store) GetDHTTimestamp(cid string) (int64, error) {
	if cid == "" {
		return 0, nil
	}
	key := []byte(prefixDHT + cid)
	var ts int64
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return err
		}
		return item.Value(func(v []byte) error {
			var parseErr error
			ts, parseErr = strconv.ParseInt(string(v), 10, 64)
			return parseErr
		})
	})
	return ts, err
}

// ============================================================
// 统计与清理
// ============================================================

// CountCIDs 返回 CID 索引数量
func (s *Store) CountCIDs() (int64, error) {
	return s.countPrefix(prefixCID)
}

// CountMetas 返回元数据索引数量
func (s *Store) CountMetas() (int64, error) {
	return s.countPrefix(prefixMeta)
}

// countPrefix 统计指定前缀的键数量
func (s *Store) countPrefix(prefix string) (int64, error) {
	var count int64
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		p := []byte(prefix)
		for it.Seek(p); it.ValidForPrefix(p); it.Next() {
			count++
		}
		return nil
	})
	return count, err
}

// Close 关闭索引存储（不关闭 Badger，因为由外部管理）
func (s *Store) Close() error {
	return nil
}

// ============================================================
// 向后兼容：保留旧版 Entry 结构和方法（供 bridge/fetcher 过渡使用）
// ============================================================

// Entry 旧版索引条目结构（向后兼容）
// 新代码应使用两层索引方法
type Entry struct {
	CID        string `json:"cid"`
	DataTXID   string `json:"data_txid"`
	BundleTXID string `json:"bundle_txid"`
	Height     int64  `json:"height"`
	DataSize   int64  `json:"data_size"`
	RawIndex   []byte `json:"raw_index,omitempty"`
	StoredAt   int64  `json:"stored_at"`
}

// Put 写入一条索引条目（向后兼容：同时写入两层索引）
func (s *Store) Put(entry *Entry) error {
	if entry == nil {
		return fmt.Errorf("index: entry is nil")
	}
	if entry.CID == "" {
		return fmt.Errorf("index: CID must not be empty")
	}

	entry.StoredAt = time.Now().Unix()

	// 写入第一层：CID → metaTxID
	if err := s.IndexCID(entry.CID, entry.DataTXID); err != nil {
		return err
	}

	// 写入第二层：metaTxID → params
	bundleTXID := entry.BundleTXID
	if bundleTXID == "" {
		bundleTXID = "none"
	}
	params := map[string]string{
		"dataTxId":    entry.DataTXID,
		"bundleTxId":  bundleTXID,
		"blockHeight": strconv.FormatInt(entry.Height, 10),
		"dataSize":    strconv.FormatInt(entry.DataSize, 10),
		"rootCid":     entry.CID,
		"verified":    "none",
		"verifiedAt":  "0",
	}
	return s.IndexMeta(entry.DataTXID, params)
}

// Get 根据 CID 获取索引条目（向后兼容：从两层索引重建 Entry）
func (s *Store) Get(cid string) (*Entry, error) {
	if cid == "" {
		return nil, fmt.Errorf("index: CID must not be empty")
	}

	// 先从第一层获取 metaTxID 列表
	metaIDs, err := s.GetCIDMetaIDs(cid)
	if err != nil {
		return nil, err
	}
	if len(metaIDs) == 0 {
		return nil, nil
	}

	// 取第一个 metaTxID，从第二层获取详情
	params, err := s.GetMeta(metaIDs[0])
	if err != nil {
		return nil, err
	}
	if params == nil {
		return nil, nil
	}

	height, _ := strconv.ParseInt(params["blockHeight"], 10, 64)
	dataSize, _ := strconv.ParseInt(params["dataSize"], 10, 64)

	entry := &Entry{
		CID:        cid,
		DataTXID:   params["dataTxId"],
		BundleTXID: params["bundleTxId"],
		Height:     height,
		DataSize:   dataSize,
		StoredAt:   time.Now().Unix(),
	}

	if entry.BundleTXID == "none" {
		entry.BundleTXID = ""
	}

	return entry, nil
}

// Has 检查 CID 是否已索引（委托给 HasCID）
func (s *Store) Has(cid string) (bool, error) {
	return s.HasCID(cid)
}

// Delete 删除一条 CID 索引
func (s *Store) Delete(cid string) error {
	return s.DeleteCID(cid)
}

// BatchPut 批量写入索引条目
func (s *Store) BatchPut(entries []*Entry) error {
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()

	for _, entry := range entries {
		if entry == nil || entry.CID == "" {
			continue
		}
		entry.StoredAt = time.Now().Unix()

		// 写入第一层
		cidKey := []byte(prefixCID + entry.CID)
		var existing []string
		item, getErr := s.db.NewTransaction(false).Get(cidKey)
		if getErr == nil {
			item.Value(func(v []byte) error {
				json.Unmarshal(v, &existing)
				return nil
			})
		}

		existing = append(existing, entry.DataTXID)
		cidVal, _ := json.Marshal(existing)
		wb.Set(cidKey, cidVal)

		// 写入第二层
		bundleTXID := entry.BundleTXID
		if bundleTXID == "" {
			bundleTXID = "none"
		}
		params := map[string]string{
			"dataTxId":    entry.DataTXID,
			"bundleTxId":  bundleTXID,
			"blockHeight": strconv.FormatInt(entry.Height, 10),
			"dataSize":    strconv.FormatInt(entry.DataSize, 10),
			"rootCid":     entry.CID,
			"verified":    "none",
			"verifiedAt":  "0",
		}
		metaKey := []byte(prefixMeta + entry.DataTXID)
		metaVal, _ := json.Marshal(params)
		wb.Set(metaKey, metaVal)
	}

	if err := wb.Flush(); err != nil {
		return fmt.Errorf("index: batch flush: %w", err)
	}

	log.Debug("索引：批量写入 %d 条", len(entries))
	return nil
}

// Count 返回 CID 索引条目总数（近似）
func (s *Store) Count() (int64, error) {
	return s.CountCIDs()
}

// DB 返回底层 Badger 实例（供高级用户使用）
func (s *Store) DB() *badger.DB {
	return s.db
}
