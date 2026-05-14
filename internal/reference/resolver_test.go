package reference

import (
	"context"
	"testing"
	"time"

	sdkmeta "github.com/LWDJD/ipfar-sdk/verify/metadata"

	"github.com/lwdjd/IPFAR/internal/cache"
	"github.com/lwdjd/IPFAR/internal/download"
)

func TestNewResolver(t *testing.T) {
	r := NewResolver(Config{
		Timeout: 30 * time.Second,
	})
	if r == nil {
		t.Fatal("NewResolver 不应返回 nil")
	}
	if r.config.Timeout != 30*time.Second {
		t.Errorf("Timeout 不匹配")
	}
}

func TestResolveNoReference(t *testing.T) {
	r := NewResolver(Config{
		Timeout: 30 * time.Second,
	})

	meta := &sdkmeta.Metadata{
		RootCID:  "bafy123",
		DataTXID: "tx-main",
		DataSize: 1000,
	}

	// 没有 reference 的元数据应返回空结果
	result, err := r.Resolve(context.Background(), meta)
	if err != nil {
		t.Fatalf("Resolve 不应返回错误: %v", err)
	}
	if result == nil {
		t.Fatal("Resolve 不应返回 nil result")
	}
	// 主 TXID 在被引用列表中有 1 个条目（自身）
	if len(result.ReferencedTXIDs) != 1 {
		t.Errorf("无引用时 ReferencedTXIDs 应有 1 个（主 TXID），得到 %d", len(result.ReferencedTXIDs))
	}
	if result.ReferencedTXIDs[0] != "tx-main" {
		t.Errorf("ReferencedTXIDs[0] 应为 tx-main，得到 %s", result.ReferencedTXIDs[0])
	}
}

func TestResolveEmptyReference(t *testing.T) {
	r := NewResolver(Config{
		Timeout: 30 * time.Second,
	})

	emptyRef := make(sdkmeta.ReferenceMap)
	meta := &sdkmeta.Metadata{
		RootCID:   "bafy123",
		DataTXID:  "tx-main",
		DataSize:  1000,
		Reference: &emptyRef,
	}

	result, err := r.Resolve(context.Background(), meta)
	if err != nil {
		t.Fatalf("Resolve 不应返回错误: %v", err)
	}
	if len(result.ReferencedTXIDs) != 1 {
		t.Errorf("空引用时 ReferencedTXIDs 应有 1 个（主 TXID），得到 %d", len(result.ReferencedTXIDs))
	}
}

func TestVisitedSetPreventsCycles(t *testing.T) {
	r := NewResolver(Config{
		Timeout: 30 * time.Second,
	})

	visited := make(map[string]bool)
	visited["tx-1"] = true
	visited["tx-2"] = true

	if !r.isVisited("tx-1", visited) {
		t.Error("tx-1 应在 visited set 中")
	}
	if r.isVisited("tx-3", visited) {
		t.Error("tx-3 不应在 visited set 中")
	}

	// 添加新条目
	r.markVisited("tx-3", visited)
	if !r.isVisited("tx-3", visited) {
		t.Error("markVisited 后 tx-3 应在 visited set 中")
	}
}

func TestBuildReferenceGraph(t *testing.T) {
	r := NewResolver(Config{
		Timeout: 30 * time.Second,
	})

	ref := sdkmeta.ReferenceMap{
		"tx-ref-1": {Height: 100, CIDs: []string{"cid-a", "cid-b"}},
		"tx-ref-2": {Height: 200, CIDs: []string{"cid-c"}},
	}

	graph := r.buildGraph(ref)
	if len(graph) != 2 {
		t.Errorf("graph 应有 2 个条目，实际 %d", len(graph))
	}
	if graph["tx-ref-1"].Height != 100 {
		t.Errorf("tx-ref-1 height 应为 100，实际 %d", graph["tx-ref-1"].Height)
	}
	if len(graph["tx-ref-2"].CIDs) != 1 {
		t.Errorf("tx-ref-2 CIDs 应有 1 个，实际 %d", len(graph["tx-ref-2"].CIDs))
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Timeout != 5*time.Minute {
		t.Errorf("默认 Timeout 应为 5m，实际 %v", cfg.Timeout)
	}
	if cfg.MaxDepth != 50 {
		t.Errorf("默认 MaxDepth 应为 50，实际 %d", cfg.MaxDepth)
	}
}

func TestValidateConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{"valid", Config{Timeout: 30 * time.Second, MaxDepth: 50}, false},
		{"zero timeout uses default", Config{Timeout: 0, MaxDepth: 50}, false},
		{"zero maxdepth uses default", Config{Timeout: 30 * time.Second, MaxDepth: 0}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validateConfig(tt.config)
			if cfg.Timeout <= 0 {
				t.Error("Timeout 应 > 0")
			}
			if cfg.MaxDepth <= 0 {
				t.Error("MaxDepth 应 > 0")
			}
		})
	}
}

// 确保导入被使用
var _ = cache.DefaultCacheConfig
var _ = download.DefaultGatewayConfig
