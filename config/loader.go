package config

import (
	"embed"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/lwdjd/IPFAR/internal/log"
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
			log.Error("读取嵌入配置失败：%s, 错误：%v", embedPath, err)
			return nil, fmt.Errorf("read embedded config '%s' failed: %w", embedPath, err)
		}
		log.Debug("成功读取嵌入配置：%s", embedPath)

		// 解析到内存对象
		if err := json.Unmarshal(data, &cfg); err != nil {
			log.Error("解析嵌入配置失败：%s, 错误：%v", embedPath, err)
			return nil, fmt.Errorf("parse embedded config failed: %w", err)
		}
		log.Debug("成功解析嵌入配置：%s", embedPath)

		// 依次调用修改器
		for _, modifier := range modifiers {
			if modifier != nil {
				modifier(&cfg)
			}
		}

		// 序列化回 JSON
		newData, err := json.MarshalIndent(cfg, "", "  ")
		if err != nil {
			log.Error("序列化配置失败：%v", err)
			return nil, fmt.Errorf("marshal modified config failed: %w", err)
		}

		// 创建目录
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			log.Error("创建配置目录失败：%s, 错误：%v", filepath.Dir(path), err)
			return nil, fmt.Errorf("create config dir failed: %w", err)
		}

		// 写入磁盘
		if err := os.WriteFile(path, newData, 0644); err != nil {
			log.Error("写入配置文件失败：%s, 错误：%v", path, err)
			return nil, fmt.Errorf("write default config to disk failed: %w", err)
		}

		log.Info("默认配置文件已释放到：%s", path)

	} else {
		// --- 文件存在，直接读取 ---
		data, err := os.ReadFile(path)
		if err != nil {
			log.Error("读取磁盘配置文件失败：%s, 错误：%v", path, err)
			return nil, fmt.Errorf("read disk config failed: %w", err)
		}
		log.Debug("从磁盘读取配置文件：%s", path)
		if err := json.Unmarshal(data, &cfg); err != nil {
			log.Error("解析磁盘配置文件失败：%s, 错误：%v", path, err)
			return nil, fmt.Errorf("parse disk config failed: %w", err)
		}
		log.Info("配置文件加载成功：%s", path)
	}

	return &cfg, nil
}

// GetConfig 获取全局配置实例
func GetConfig() (*Config, error) {
	log.Info("开始加载全局配置...")
	// 只需要传入一个路径
	// 自动从 config/defaults/config.json 读取默认值
	cfg, err := Load("config.json", func(c *Config) {
		// 修改器：设置系统语言
		c.Language = lang.GetSystemLanguage()
		log.Debug("检测到系统语言：%s", c.Language)
	})

	if err != nil {
		log.Error("获取全局配置失败：%v", err)
		return nil, err
	}

	log.Info("全局配置加载完成")
	return cfg, nil
}
