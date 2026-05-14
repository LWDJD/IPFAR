package config

import (
	"github.com/leonelquinteros/gotext"
	"github.com/lwdjd/IPFAR/internal/log"
)

// Config 主配置结构体
type Config struct {
	Language     string `json:"language"`
	LogLevel     string `json:"log_level,omitempty"`      // 日志级别：debug, info, warn, error
	LogFile      string `json:"log_file,omitempty"`       // 日志文件路径（可选）
	LogToConsole bool   `json:"log_to_console,omitempty"` // 是否输出到控制台（默认 false）
	LogFormat    string `json:"log_format,omitempty"`     // 日志格式：text, json

	// 验证选项（规范 §4.1）
	VerifyPoW            bool `json:"verify_pow,omitempty"`             // PoW 验证开关
	VerifyIndex          bool `json:"verify_index,omitempty"`           // Index 完整性验证开关
	VerifyReferenceChain bool `json:"verify_reference_chain,omitempty"` // 引用链验证开关
	VerifyIntegrity      bool `json:"verify_integrity,omitempty"`       // 数据完整性验证开关

	// 安全等级预设（如果设置，将覆盖上述单独选项）
	SecurityPreset string `json:"security_preset,omitempty"` // strict, balanced, light, trusted

	// 网关配置（规范 §3）
	GatewayList              []string `json:"gateway_list,omitempty"`                // 自定义网关列表
	GatewayAllowLocal        bool     `json:"gateway_allow_local,omitempty"`         // 是否允许本地网关（默认 false）
	GatewayHealthCheck       bool     `json:"gateway_health_check,omitempty"`        // 是否启用健康检查（默认 true）
	GatewayHealthCheckSecs   int      `json:"gateway_health_check_secs,omitempty"`   // 健康检查间隔（秒，默认 60）
	GatewayTimeoutSecs       int      `json:"gateway_timeout_secs,omitempty"`        // 网关请求超时（秒，默认 30）
	GatewayMaxRetries        int      `json:"gateway_max_retries,omitempty"`         // 最大重试次数（默认 3）

	// DHT 内容发布配置（规范 P3-1）
	DHT DHTConfig `json:"dht,omitempty"`
}

// DHTConfig DHT 内容发布配置
type DHTConfig struct {
	// Enabled 是否启用 DHT 内容发布
	Enabled bool `json:"enabled,omitempty"`
	// Mode DHT 运行模式: "server" 或 "client"
	Mode string `json:"mode,omitempty"`
	// BootstrapPeers 引导节点地址列表
	BootstrapPeers []string `json:"bootstrap_peers,omitempty"`
	// ReprovideInterval 重新提供间隔（字符串格式，如 "12h"）
	ReprovideInterval string `json:"reprovide_interval,omitempty"`
	// ProvideConcurrency 提供并发数
	ProvideConcurrency int `json:"provide_concurrency,omitempty"`
	// RetryMaxAttempts 最大重试次数
	RetryMaxAttempts int `json:"retry_max_attempts,omitempty"`
	// ListenAddresses libp2p 监听地址
	ListenAddresses []string `json:"listen_addresses,omitempty"`
}

// ConfigFile 全局配置文件实例
var ConfigFile *Config

// Loc 全局语言实例
var Loc *gotext.Locale

// InitLog 根据配置初始化日志系统
func InitLog() error {
	if ConfigFile == nil {
		return log.Init(log.Config{
			Level:      log.INFO,
			FilePath:   "",
			UseConsole: false,
			UseJSON:    false,
		})
	}

	// 解析日志级别
	level := log.INFO
	switch ConfigFile.LogLevel {
	case "debug":
		level = log.DEBUG
	case "info":
		level = log.INFO
	case "warn":
		level = log.WARN
	case "error":
		level = log.ERROR
	}

	// 解析日志格式
	useJSON := ConfigFile.LogFormat == "json"

	// 初始化日志
	cfg := log.Config{
		Level:      level,
		FilePath:   ConfigFile.LogFile,
		UseConsole: ConfigFile.LogToConsole,
		UseJSON:    useJSON,
	}

	if err := log.Init(cfg); err != nil {
		return err
	}

	if ConfigFile.LogLevel != "" {
		log.Info("日志级别已设置为：%s", ConfigFile.LogLevel)
	}
	if ConfigFile.LogFile != "" {
		log.Info("日志文件路径：%s", ConfigFile.LogFile)
	}
	log.Info("控制台输出：%v", ConfigFile.LogToConsole)

	return nil
}

// GetVerifyConfig 从配置中获取验证配置
// 如果设置了 security_preset，则使用预设值覆盖单独选项
func (c *Config) GetVerifyConfig() (verifyPoW, verifyIndex, verifyRef, verifyIntegrity bool) {
	// 默认值：light 模式
	if c.SecurityPreset == "" && !c.VerifyPoW && !c.VerifyIndex && !c.VerifyReferenceChain && !c.VerifyIntegrity {
		return true, false, true, false // light: PoW + Ref chain
	}

	switch c.SecurityPreset {
	case "strict":
		return true, true, true, true
	case "balanced":
		return true, true, false, true
	case "light":
		return true, false, true, false
	case "trusted":
		return false, false, false, false
	default:
		return c.VerifyPoW, c.VerifyIndex, c.VerifyReferenceChain, c.VerifyIntegrity
	}
}
