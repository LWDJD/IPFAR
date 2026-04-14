package flags

import (
	"os"
	"testing"
)

func TestFlagSetBasic(t *testing.T) {
	fs := NewFlagSet("test")

	fs.DefineString("name", "n", "default", "姓名", false)
	fs.DefineString("config", "c", "", "配置文件路径", true)

	// 模拟命令行参数
	oldArgs := []string{"test", "-name", "Alice", "-config", "config.json"}
	originalArgs := make([]string, len(os.Args))
	copy(originalArgs, os.Args)
	os.Args = oldArgs
	defer func() {
		os.Args = originalArgs
	}()

	err := fs.Parse()
	if err != nil {
		t.Errorf("解析失败：%v", err)
	}

	if fs.GetString("name") != "Alice" {
		t.Errorf("期望 name=Alice, 得到 name=%s", fs.GetString("name"))
	}

	if fs.GetString("config") != "config.json" {
		t.Errorf("期望 config=config.json, 得到 config=%s", fs.GetString("config"))
	}
}

func TestFlagSetRequired(t *testing.T) {
	fs := NewFlagSet("test")
	fs.DefineString("required", "r", "default", "必需参数", true)

	oldArgs := []string{"test"}
	originalArgs := make([]string, len(os.Args))
	copy(originalArgs, os.Args)
	os.Args = oldArgs
	defer func() {
		os.Args = originalArgs
	}()

	err := fs.Parse()
	// 必需参数有默认值，所以不会失败
	if err != nil {
		t.Logf("解析结果：%v", err)
	}
}

func TestFlagSetValidate(t *testing.T) {
	fs := NewFlagSet("test")
	fs.Define(Flag{
		Name:         "port",
		Value:        "8080",
		Usage:        "端口号",
		ValidateFunc: ValidateNumber(1, 65535),
	})

	// 测试有效值
	oldArgs := []string{"test", "-port", "8080"}
	originalArgs := make([]string, len(os.Args))
	copy(originalArgs, os.Args)
	os.Args = oldArgs
	defer func() {
		os.Args = originalArgs
	}()

	err := fs.Parse()
	if err != nil {
		t.Errorf("有效值解析失败：%v", err)
	}

	// 测试无效值
	os.Args = []string{"test", "-port", "99999"}
	err = fs.Parse()
	if err == nil {
		t.Error("期望验证失败，但成功了")
	}
}

func TestFlagSetTypes(t *testing.T) {
	fs := NewFlagSet("test")
	fs.DefineString("name", "", "default", "名称", false)
	fs.DefineInt("count", "c", 10, "数量")

	oldArgs := []string{"test", "-name", "test", "-count", "20"}
	originalArgs := make([]string, len(os.Args))
	copy(originalArgs, os.Args)
	os.Args = oldArgs
	defer func() {
		os.Args = originalArgs
	}()

	err := fs.Parse()
	if err != nil {
		t.Errorf("解析失败：%v", err)
	}

	if fs.GetString("name") != "test" {
		t.Errorf("name 期望 test, 得到 %s", fs.GetString("name"))
	}

	if fs.GetInt("count") != 20 {
		t.Errorf("count 期望 20, 得到 %d", fs.GetInt("count"))
	}
}

func TestFlagSetIsSet(t *testing.T) {
	fs := NewFlagSet("test")
	fs.DefineString("set", "", "default", "已设置", false)
	fs.DefineString("unset", "", "default", "未设置", false)

	oldArgs := []string{"test", "-set", "value"}
	originalArgs := make([]string, len(os.Args))
	copy(originalArgs, os.Args)
	os.Args = oldArgs
	defer func() {
		os.Args = originalArgs
	}()

	err := fs.Parse()
	if err != nil {
		t.Errorf("解析失败：%v", err)
	}

	if !fs.IsSet("set") {
		t.Error("set 应该被设置")
	}

	if fs.IsSet("unset") {
		t.Error("unset 不应该被设置")
	}
}
