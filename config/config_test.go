package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/lwdjd/IPFAR/internal/log"
)

func TestConfigJSONSerialization(t *testing.T) {
	cfg := Config{
		Language:     "en_US",
		LogLevel:     "info",
		LogFile:      "logs/ipfar.log",
		LogToConsole: false,
		LogFormat:    "text",
	}

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		t.Fatalf("序列化配置失败：%v", err)
	}

	var parsed Config
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("反序列化配置失败：%v", err)
	}

	if parsed.Language != cfg.Language {
		t.Errorf("Language 不匹配：期望 %s，得到 %s", cfg.Language, parsed.Language)
	}
	if parsed.LogLevel != cfg.LogLevel {
		t.Errorf("LogLevel 不匹配：期望 %s，得到 %s", cfg.LogLevel, parsed.LogLevel)
	}
	if parsed.LogFile != cfg.LogFile {
		t.Errorf("LogFile 不匹配：期望 %s，得到 %s", cfg.LogFile, parsed.LogFile)
	}
	if parsed.LogToConsole != cfg.LogToConsole {
		t.Errorf("LogToConsole 不匹配：期望 %v，得到 %v", cfg.LogToConsole, parsed.LogToConsole)
	}
	if parsed.LogFormat != cfg.LogFormat {
		t.Errorf("LogFormat 不匹配：期望 %s，得到 %s", cfg.LogFormat, parsed.LogFormat)
	}
}

func TestConfigJSONOmitEmpty(t *testing.T) {
	cfg := Config{
		Language: "en_US",
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("序列化配置失败：%v", err)
	}

	s := string(data)
	if s == "" {
		t.Fatal("序列化结果不应为空")
	}

	var parsed Config
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("反序列化配置失败：%v", err)
	}

	if parsed.Language != "en_US" {
		t.Errorf("Language 应为 en_US，得到 %s", parsed.Language)
	}
}

func TestInitLogWithNilConfig(t *testing.T) {
	origConfig := ConfigFile
	ConfigFile = nil
	defer func() { ConfigFile = origConfig }()

	log.Init(log.Config{Level: log.INFO, FilePath: "", UseConsole: false})

	err := InitLog()
	if err != nil {
		t.Errorf("InitLog with nil ConfigFile 不应返回错误：%v", err)
	}

	log.Close()
}

func TestInitLogWithConfig(t *testing.T) {
	origConfig := ConfigFile
	defer func() { ConfigFile = origConfig }()

	log.Init(log.Config{Level: log.INFO, FilePath: "", UseConsole: false})

	ConfigFile = &Config{
		Language:     "en_US",
		LogLevel:     "debug",
		LogFile:      "",
		LogToConsole: false,
		LogFormat:    "text",
	}

	err := InitLog()
	if err != nil {
		t.Errorf("InitLog with Config 不应返回错误：%v", err)
	}

	log.Close()
}

func TestInitLogWithJSONFormat(t *testing.T) {
	origConfig := ConfigFile
	defer func() { ConfigFile = origConfig }()

	log.Init(log.Config{Level: log.INFO, FilePath: "", UseConsole: false})

	ConfigFile = &Config{
		Language:     "en_US",
		LogLevel:     "info",
		LogFile:      "",
		LogToConsole: false,
		LogFormat:    "json",
	}

	err := InitLog()
	if err != nil {
		t.Errorf("InitLog with JSON format 不应返回错误：%v", err)
	}

	log.Close()
}

func TestInitLogAllLevels(t *testing.T) {
	levels := []struct {
		level    string
		expected log.Level
	}{
		{"debug", log.DEBUG},
		{"info", log.INFO},
		{"warn", log.WARN},
		{"error", log.ERROR},
		{"", log.INFO},
		{"unknown", log.INFO},
	}

	origConfig := ConfigFile
	defer func() { ConfigFile = origConfig }()

	for _, tt := range levels {
		log.Init(log.Config{Level: log.INFO, FilePath: "", UseConsole: false})

		ConfigFile = &Config{
			Language:     "en_US",
			LogLevel:     tt.level,
			LogFile:      "",
			LogToConsole: false,
			LogFormat:    "text",
		}

		err := InitLog()
		if err != nil {
			t.Errorf("InitLog with level %q 不应返回错误：%v", tt.level, err)
		}

		log.Close()
	}
}

func TestLoadFromDisk(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "ipfar_config_test")
	os.MkdirAll(tmpDir, 0755)
	defer os.RemoveAll(tmpDir)

	configPath := filepath.Join(tmpDir, "test_config.json")

	cfg := Config{
		Language:     "zh_CN",
		LogLevel:     "warn",
		LogFile:      "logs/test.log",
		LogToConsole: true,
		LogFormat:    "json",
	}

	data, _ := json.MarshalIndent(cfg, "", "  ")
	os.WriteFile(configPath, data, 0644)

	log.Init(log.Config{Level: log.INFO, FilePath: "", UseConsole: false})
	defer log.Close()

	loaded, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load 从磁盘加载失败：%v", err)
	}

	if loaded.Language != "zh_CN" {
		t.Errorf("Language 不匹配：期望 zh_CN，得到 %s", loaded.Language)
	}
	if loaded.LogLevel != "warn" {
		t.Errorf("LogLevel 不匹配：期望 warn，得到 %s", loaded.LogLevel)
	}
	if loaded.LogFile != "logs/test.log" {
		t.Errorf("LogFile 不匹配：期望 logs/test.log，得到 %s", loaded.LogFile)
	}
	if !loaded.LogToConsole {
		t.Error("LogToConsole 应为 true")
	}
	if loaded.LogFormat != "json" {
		t.Errorf("LogFormat 不匹配：期望 json，得到 %s", loaded.LogFormat)
	}
}

func TestLoadFromEmbedded(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "ipfar_embedded_test")
	os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)
	os.MkdirAll(tmpDir, 0755)

	log.Init(log.Config{Level: log.INFO, FilePath: "", UseConsole: false})
	defer log.Close()

	origDir, _ := os.Getwd()
	err := os.Chdir(tmpDir)
	if err != nil {
		t.Fatalf("Chdir 失败：%v", err)
	}
	defer os.Chdir(origDir)

	loaded, err := Load("config.json")
	if err != nil {
		t.Fatalf("Load 从嵌入资源释放失败：%v", err)
	}

	if loaded.Language != "en_US" {
		t.Errorf("嵌入默认配置 Language 应为 en_US，得到 %s", loaded.Language)
	}
	if loaded.LogLevel != "info" {
		t.Errorf("嵌入默认配置 LogLevel 应为 info，得到 %s", loaded.LogLevel)
	}

	cwd, _ := os.Getwd()
	configPath := filepath.Join(cwd, "config.json")
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Errorf("配置文件应已释放到磁盘：%s", configPath)
	}
}

func TestLoadWithModifier(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "ipfar_config_modifier_test")
	os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)
	os.MkdirAll(tmpDir, 0755)

	log.Init(log.Config{Level: log.INFO, FilePath: "", UseConsole: false})
	defer log.Close()

	origDir, _ := os.Getwd()
	err := os.Chdir(tmpDir)
	if err != nil {
		t.Fatalf("Chdir 失败：%v", err)
	}
	defer os.Chdir(origDir)

	loaded, err := Load("config.json", func(c *Config) {
		c.Language = "zh_CN"
	})
	if err != nil {
		t.Fatalf("Load with modifier 失败：%v", err)
	}

	if loaded.Language != "zh_CN" {
		t.Errorf("Modifier 应将 Language 改为 zh_CN，得到 %s", loaded.Language)
	}

	cwd, _ := os.Getwd()
	configPath := filepath.Join(cwd, "config.json")
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Errorf("配置文件应已写入磁盘：%s", configPath)
	}
}
