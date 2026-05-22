// Package index 提供 CID → Arweave 位置的元数据索引
//
// 该模块使用 Badger 存储 CID 到 Arweave 交易位置的映射，
// 支持 Bitswap 按需拉取：收到 WantList → 查索引 → 从 Arweave 按需下载。
//
// 键格式: "idx:" + CID (base32 字符串)
// 值格式: JSON 序列化的 Entry
package index

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"

	"github.com/lwdjd/IPFAR/internal/log"
)

// 键前缀常量
const keyPrefix = "idx:"

// Entry 索引条目：一个 CID 在 Arweave 上的位置信息
type Entry struct {
	CID        string `json:"cid"`         // IPFS CID (base32)
	DataTXID   string `json:"data_txid"`   // Arweave TX ID 或 Bundle Item ID
	BundleTXID string `json:"bundle_txid"` // 可选，跨 Bundle 时用
	Height     int64  `json:"height"`      // 区块高度（-1 表示同 Bundle）
	DataSize   int64  `json:"data_size"`   // 数据大小（字节）
	RawIndex   []byte `json:"raw_index"`   // CAR v2 Index 序列化（用于快速定位块偏移）
	StoredAt   int64  `json:"stored_at"`   // 索引时间戳（Unix 秒）
}

// Store CID → Arweave 位置索引存储
// 线程安全（Badger 原生支持并发读写）
type Store struct {
	db *badger.DB
}

// NewStore 创建新的索引存储
// db 为已打开的 Badger 实例（复用）
func NewStore(db *badger.DB) *Store {
	return &Store{db: db}
}

// Put 写入一条索引条目
func (s *Store) Put(entry *Entry) error {
	if entry == nil {
		return fmt.Errorf("index: entry is nil")
	}
	if entry.CID == "" {
		return fmt.Errorf("index: CID must not be empty")
	}

	entry.StoredAt = time.Now().Unix()

	val, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("index: marshal entry: %w", err)
	}

	key := makeKey(entry.CID)
	err = s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, val)
	})
	if err != nil {
		return fmt.Errorf("index: put %s: %w", entry.CID, err)
	}

	log.Debug("索引：写入 CID=%s → tx=%s height=%d size=%d",
		entry.CID, entry.DataTXID, entry.Height, entry.DataSize)
	return nil
}

// Get 根据 CID 获取索引条目
// 返回 (entry, found, error)
func (s *Store) Get(cid string) (*Entry, error) {
	if cid == "" {
		return nil, fmt.Errorf("index: CID must not be empty")
	}

	key := makeKey(cid)
	var entry *Entry

	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return err
		}
		return item.Value(func(v []byte) error {
			var e Entry
			if err := json.Unmarshal(v, &e); err != nil {
				return fmt.Errorf("index: unmarshal entry: %w", err)
			}
			entry = &e
			return nil
		})
	})
	if err != nil {
		return nil, fmt.Errorf("index: get %s: %w", cid, err)
	}

	return entry, nil
}

// Has 检查 CID 是否已索引
func (s *Store) Has(cid string) (bool, error) {
	if cid == "" {
		return false, nil
	}

	key := makeKey(cid)
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

// Delete 删除一条索引
func (s *Store) Delete(cid string) error {
	if cid == "" {
		return nil
	}

	key := makeKey(cid)
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
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
		val, err := json.Marshal(entry)
		if err != nil {
			return fmt.Errorf("index: marshal entry %s: %w", entry.CID, err)
		}
		key := makeKey(entry.CID)
		if err := wb.Set(key, val); err != nil {
			return fmt.Errorf("index: batch put %s: %w", entry.CID, err)
		}
	}

	if err := wb.Flush(); err != nil {
		return fmt.Errorf("index: batch flush: %w", err)
	}

	log.Debug("索引：批量写入 %d 条", len(entries))
	return nil
}

// Count 返回索引中的条目总数（近似）
func (s *Store) Count() (int64, error) {
	var count int64
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(keyPrefix)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			count++
		}
		return nil
	})
	return count, err
}

// Close 关闭索引存储（不关闭 Badger，因为由外部管理）
func (s *Store) Close() error {
	// Badger 实例由外部管理，这里不关闭
	return nil
}

// ============================================================
// 工具函数
// ============================================================

// makeKey 构建 Badger 键
func makeKey(cid string) []byte {
	return []byte(keyPrefix + cid)
}
