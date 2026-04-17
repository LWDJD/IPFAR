package log

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoggerBasic(t *testing.T) {
	cfg := Config{
		Level:    DEBUG,
		FilePath: "",
		UseJSON:  false,
	}
	if err := Init(cfg); err != nil {
		t.Fatalf("初始化日志失败：%v", err)
	}
	defer Close()

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

	name := "测试"
	count := 42
	Info("用户：%s, 数量：%d", name, count)
}

func TestLoggerFile(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "ipfar_log_test")
	logFile := filepath.Join(tmpDir, "test.log")

	os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)

	cfg := Config{
		Level:         INFO,
		FilePath:      logFile,
		UseJSON:       false,
		FlushInterval: 1 * time.Second,
	}
	if err := Init(cfg); err != nil {
		t.Fatalf("初始化日志失败：%v", err)
	}
	defer Close()

	Info("测试文件日志")
	Error("测试错误日志")

	Close()

	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("读取日志目录失败：%v", err)
	}

	found := false
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "test_") && strings.HasSuffix(entry.Name(), ".log") {
			found = true
			content, err := os.ReadFile(filepath.Join(tmpDir, entry.Name()))
			if err != nil {
				t.Fatalf("读取日志文件失败：%v", err)
			}
			if len(content) == 0 {
				t.Fatal("日志文件为空")
			}
			t.Logf("日志文件内容：%s", string(content))
			break
		}
	}

	if !found {
		t.Fatal("未找到带日期时间命名的日志文件")
	}
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
	cfg1 := Config{
		Level:    INFO,
		FilePath: "",
	}
	if err := Init(cfg1); err != nil {
		t.Fatalf("第一次初始化失败：%v", err)
	}
	Info("第一次初始化")

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

func TestCloseSafety(t *testing.T) {
	cfg := Config{
		Level:    INFO,
		FilePath: "",
	}
	if err := Init(cfg); err != nil {
		t.Fatalf("初始化日志失败：%v", err)
	}

	Info("关闭前日志")

	Close()

	Info("关闭后日志 - 不应 panic")
	Debug("关闭后日志 - 不应 panic")
	Warn("关闭后日志 - 不应 panic")
	Error("关闭后日志 - 不应 panic")
}

func TestCloseNil(t *testing.T) {
	globalLogger = nil

	err := Close()
	if err != nil {
		t.Errorf("Close nil logger 不应返回错误：%v", err)
	}
}

func TestDoubleClose(t *testing.T) {
	cfg := Config{
		Level:    INFO,
		FilePath: "",
	}
	if err := Init(cfg); err != nil {
		t.Fatalf("初始化日志失败：%v", err)
	}

	Info("测试双重关闭")

	Close()
	Close()
}

func TestUseConsole(t *testing.T) {
	cfg := Config{
		Level:      INFO,
		FilePath:   "",
		UseConsole: true,
	}
	if err := Init(cfg); err != nil {
		t.Fatalf("初始化日志失败：%v", err)
	}
	defer Close()

	Info("控制台输出测试")
}

func TestUseConsoleDisabled(t *testing.T) {
	cfg := Config{
		Level:      INFO,
		FilePath:   "",
		UseConsole: false,
	}
	if err := Init(cfg); err != nil {
		t.Fatalf("初始化日志失败：%v", err)
	}
	defer Close()

	Info("不应输出到控制台")
}

func TestMaxFiles(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "ipfar_maxfiles_test")
	logFile := filepath.Join(tmpDir, "app.log")

	os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)

	os.MkdirAll(tmpDir, 0755)

	for i := 0; i < 7; i++ {
		f, err := os.Create(filepath.Join(tmpDir, fmt.Sprintf("app_2026-01-%02d_12-00-00_%d.log", i+1, i+1)))
		if err != nil {
			t.Fatalf("创建测试文件失败：%v", err)
		}
		f.Close()
		time.Sleep(10 * time.Millisecond)
	}

	entriesBefore, _ := os.ReadDir(tmpDir)
	if len(entriesBefore) != 7 {
		t.Fatalf("预创建文件数量不对：期望 7，得到 %d", len(entriesBefore))
	}

	cfg := Config{
		Level:         INFO,
		FilePath:      logFile,
		MaxFiles:      3,
		FlushInterval: 1 * time.Second,
	}
	if err := Init(cfg); err != nil {
		t.Fatalf("初始化日志失败：%v", err)
	}

	Info("测试 MaxFiles")

	Close()

	entriesAfter, _ := os.ReadDir(tmpDir)
	if len(entriesAfter) > 4 {
		t.Errorf("清理后文件数量不对：期望最多 4（3 旧 + 1 新），得到 %d", len(entriesAfter))
	}
}

func TestSetFile(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "ipfar_setfile_test")

	os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)

	cfg := Config{
		Level:    INFO,
		FilePath: "",
	}
	if err := Init(cfg); err != nil {
		t.Fatalf("初始化日志失败：%v", err)
	}
	defer Close()

	newFile := filepath.Join(tmpDir, "dynamic.log")
	if err := SetFile(newFile); err != nil {
		t.Fatalf("SetFile 失败：%v", err)
	}

	Info("动态设置文件后的日志")
}

func TestDisableFile(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "ipfar_disablefile_test")
	logFile := filepath.Join(tmpDir, "test.log")

	os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)

	cfg := Config{
		Level:         INFO,
		FilePath:      logFile,
		FlushInterval: 1 * time.Second,
	}
	if err := Init(cfg); err != nil {
		t.Fatalf("初始化日志失败：%v", err)
	}
	defer Close()

	Info("文件启用时的日志")

	DisableFile()

	Info("文件禁用后的日志")
}

func TestLevelString(t *testing.T) {
	tests := []struct {
		level    Level
		expected string
	}{
		{DEBUG, "DEBUG"},
		{INFO, "INFO"},
		{WARN, "WARN"},
		{ERROR, "ERROR"},
		{FATAL, "FATAL"},
		{Level(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		result := tt.level.String()
		if result != tt.expected {
			t.Errorf("Level(%d).String() = %s, 期望 %s", tt.level, result, tt.expected)
		}
	}
}
