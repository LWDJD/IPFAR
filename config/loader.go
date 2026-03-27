package config

import (
	"embed"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/lwdjd/IPFAR/lang"
)

//go:embed defaults
var defaultsFS embed.FS

// ConfigModifier 配置修改器接口
type ConfigModifier func(cfg *Config)

// Load 加载配置文件
// path: 相对于程序根目录的路径 (如 "config/app.json")
// 内部会自动寻找 config/defaults/config/app.json 作为默认配置
func Load(path string, modifiers ...ConfigModifier) (*Config, error) {
	var cfg Config

	// 1. 检查磁盘文件是否存在
	if _, err := os.Stat(path); os.IsNotExist(err) {
		// --- 文件不存在，执行释放逻辑 ---
		// fmt.Println("testA")
		// 构造嵌入资源的路径: defaults + path
		// 例如 path="config/app.json" -> embedPath="defaults/config/app.json"
		embedPath := filepath.Join("defaults", path)

		// 为了兼容 embed.FS，将 Windows 的反斜杠转换为正斜杠
		embedPath = filepath.ToSlash(embedPath)

		// 从二进制嵌入中读取
		data, err := defaultsFS.ReadFile(embedPath)
		if err != nil {
			return nil, fmt.Errorf("read embedded config '%s' failed: %w", embedPath, err)
		}

		// 解析到内存对象
		if err := json.Unmarshal(data, &cfg); err != nil {
			return nil, fmt.Errorf("parse embedded config failed: %w", err)
		}

		// 依次调用修改器
		for _, modifier := range modifiers {
			if modifier != nil {
				modifier(&cfg)
			}
		}

		// 序列化回 JSON
		newData, err := json.MarshalIndent(cfg, "", "  ")
		if err != nil {
			return nil, fmt.Errorf("marshal modified config failed: %w", err)
		}

		// 创建目录
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return nil, fmt.Errorf("create config dir failed: %w", err)
		}

		// 写入磁盘
		if err := os.WriteFile(path, newData, 0644); err != nil {
			return nil, fmt.Errorf("write default config to disk failed: %w", err)
		}

		fmt.Printf("[Config] Default config released to: %s\n", path)

	} else {
		// fmt.Println("testB")
		// --- 文件存在，直接读取 ---
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read disk config failed: %w", err)
		}
		if err := json.Unmarshal(data, &cfg); err != nil {
			return nil, fmt.Errorf("parse disk config failed: %w", err)
		}
	}

	return &cfg, nil
}

// GetConfig 获取全局配置实例
func GetConfig() (*Config, error) {
	// 只需要传入一个路径
	// 自动从 config/defaults/config.json 读取默认值
	cfg, err := Load("config.json", func(c *Config) {
		// 修改器：设置系统语言
		c.Language = lang.GetSystemLanguage()
	})

	if err != nil {
		return nil, err
	}

	return cfg, nil
}
