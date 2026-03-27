package config

import (
	"github.com/leonelquinteros/gotext"
)

// Config 主配置结构体
type Config struct {
	Language string `json:"language"`
}

// ConfigFile 全局配置文件实例
var ConfigFile *Config

// Loc 全局语言实例
var Loc *gotext.Locale
