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
			UseConsole: true,
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
