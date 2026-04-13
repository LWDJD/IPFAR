package log

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoggerBasic(t *testing.T) {
	// 初始化日志
	cfg := Config{
		Level:    DEBUG,
		FilePath: "",
		UseJSON:  false,
	}
	if err := Init(cfg); err != nil {
		t.Fatalf("初始化日志失败：%v", err)
	}
	defer Close()

	// 测试各个级别的日志输出
	Debug("这是一条 DEBUG 日志")
	Info("这是一条 INFO 日志")
	Warn("这是一条 WARN 日志")
	Error("这是一条 ERROR 日志")
}

func TestLoggerLevel(t *testing.T) {
	cfg := Config{
		Level:    DEBUG,
		FilePath: "",
	}
	if err := Init(cfg); err != nil {
		t.Fatalf("初始化日志失败：%v", err)
	}
	defer Close()

	// 测试日志级别设置
	SetLevel(DEBUG)
	Debug("DEBUG 级别应该显示")

	SetLevel(INFO)
	Debug("INFO 级别不应该显示这条 DEBUG")
	Info("INFO 级别应该显示")

	SetLevel(WARN)
	Debug("WARN 级别不应该显示这条 DEBUG")
	Info("WARN 级别不应该显示这条 INFO")
	Warn("WARN 级别应该显示")

	SetLevel(ERROR)
	Warn("ERROR 级别不应该显示这条 WARN")
	Error("ERROR 级别应该显示")

	// 恢复默认级别
	SetLevel(INFO)
}

func TestLoggerPrefix(t *testing.T) {
	cfg := Config{
		Level:    INFO,
		FilePath: "",
	}
	if err := Init(cfg); err != nil {
		t.Fatalf("初始化日志失败：%v", err)
	}
	defer Close()

	SetPrefix("TEST")
	Info("带前缀的日志")
	ResetPrefix()
	Info("无前缀的日志")
}

func TestLoggerFormat(t *testing.T) {
	cfg := Config{
		Level:    INFO,
		FilePath: "",
	}
	if err := Init(cfg); err != nil {
		t.Fatalf("初始化日志失败：%v", err)
	}
	defer Close()

	// 测试格式化输出
	name := "测试"
	count := 42
	Info("用户：%s, 数量：%d", name, count)
}

func TestLoggerFile(t *testing.T) {
	// 创建临时目录
	tmpDir := filepath.Join(os.TempDir(), "log_test")
	logFile := filepath.Join(tmpDir, "test.log")

	// 清理旧文件
	os.RemoveAll(tmpDir)

	cfg := Config{
		Level:         INFO,
		FilePath:      logFile,
		UseJSON:       false,
		FlushInterval: 1 * time.Second,
	}
	if err := Init(cfg); err != nil {
		t.Fatalf("初始化日志失败：%v", err)
	}

	Info("测试文件日志")
	Error("测试错误日志")

	// 等待刷盘
	time.Sleep(2 * time.Second)

	Close()

	// 检查文件是否存在
	if _, err := os.Stat(logFile); os.IsNotExist(err) {
		t.Fatalf("日志文件未创建：%s", logFile)
	}

	// 读取文件内容
	content, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("读取日志文件失败：%v", err)
	}

	if len(content) == 0 {
		t.Fatal("日志文件为空")
	}

	t.Logf("日志文件内容：%s", string(content))
}

func TestLoggerJSON(t *testing.T) {
	cfg := Config{
		Level:    INFO,
		FilePath: "",
		UseJSON:  true,
	}
	if err := Init(cfg); err != nil {
		t.Fatalf("初始化日志失败：%v", err)
	}
	defer Close()

	Info("JSON 格式测试")
}

func TestLoggerReinit(t *testing.T) {
	// 第一次初始化
	cfg1 := Config{
		Level:    INFO,
		FilePath: "",
	}
	if err := Init(cfg1); err != nil {
		t.Fatalf("第一次初始化失败：%v", err)
	}
	Info("第一次初始化")

	// 第二次初始化（应该关闭第一次的）
	cfg2 := Config{
		Level:    DEBUG,
		FilePath: "",
	}
	if err := Init(cfg2); err != nil {
		t.Fatalf("第二次初始化失败：%v", err)
	}
	defer Close()
	Debug("第二次初始化")
}
