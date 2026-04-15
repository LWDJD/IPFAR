package cmd

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
)

func TestCommandBasic(t *testing.T) {
	// 创建测试命令
	root := &Command{
		Name:  "test",
		Short: "测试命令",
	}

	executed := false
	root.AddCommand(&Command{
		Name:  "hello",
		Short: "打招呼",
		Run: func(args []string) error {
			executed = true
			return nil
		},
	})

	// 测试执行命令
	err := root.Execute([]string{"hello"})
	if err != nil {
		t.Errorf("执行命令失败：%v", err)
	}

	if !executed {
		t.Error("命令未被执行")
	}
}

func TestCommandNotFound(t *testing.T) {
	root := &Command{
		Name:  "test",
		Short: "测试命令",
	}

	err := root.Execute([]string{"unknown"})
	if err == nil {
		t.Error("期望命令未找到错误，但成功了")
	}
}

func TestCommandArgs(t *testing.T) {
	root := &Command{
		Name:  "test",
		Short: "测试命令",
	}

	// 测试需要 2 个参数的命令
	root.AddCommand(&Command{
		Name:  "set",
		Short: "设置",
		Args:  2,
		Run: func(args []string) error {
			if len(args) != 2 {
				t.Errorf("期望 2 个参数，得到 %d 个", len(args))
			}
			return nil
		},
	})

	// 正确参数数量
	err := root.Execute([]string{"set", "key", "value"})
	if err != nil {
		t.Errorf("执行失败：%v", err)
	}

	// 错误参数数量
	err = root.Execute([]string{"set", "key"})
	if err == nil {
		t.Error("期望参数数量错误，但成功了")
	}
}

func TestSubcommands(t *testing.T) {
	root := &Command{
		Name:  "test",
		Short: "测试命令",
	}

	parent := &Command{
		Name:  "parent",
		Short: "父命令",
	}

	parent.AddCommand(&Command{
		Name:  "child",
		Short: "子命令",
		Run: func(args []string) error {
			return nil
		},
	})

	root.AddCommand(parent)

	// 查找子命令
	found := root.FindCommand("parent")
	if found == nil {
		t.Error("未找到父命令")
	}

	if len(found.Subcommands) != 1 {
		t.Errorf("期望 1 个子命令，得到 %d 个", len(found.Subcommands))
	}
}

func TestHiddenCommand(t *testing.T) {
	root := &Command{
		Name:  "test",
		Short: "测试命令",
	}

	root.AddCommand(&Command{
		Name:   "visible",
		Short:  "可见命令",
		Hidden: false,
	})

	root.AddCommand(&Command{
		Name:   "hidden",
		Short:  "隐藏命令",
		Hidden: true,
	})

	// 打印帮助（应该只显示可见命令）
	root.PrintHelp()

	// 隐藏命令仍然可以执行
	err := root.Execute([]string{"hidden"})
	if err != nil {
		t.Errorf("隐藏命令应该可以执行：%v", err)
	}
}

func TestRegisterCommands(t *testing.T) {
	// 测试注册命令
	RegisterCommands()

	// 检查根命令是否有子命令
	if len(RootCommand.Subcommands) == 0 {
		t.Error("根命令应该有子命令")
	}

	// 查找特定命令
	versionCmd := RootCommand.FindCommand("version")
	if versionCmd == nil {
		t.Error("未找到 version 命令")
	}

	helpCmd := RootCommand.FindCommand("help")
	if helpCmd == nil {
		t.Error("未找到 help 命令")
	}

	initCmd := RootCommand.FindCommand("init")
	if initCmd == nil {
		t.Error("未找到 init 命令")
	}

	serveCmd := RootCommand.FindCommand("serve")
	if serveCmd == nil {
		t.Error("未找到 serve 命令")
	}

	statusCmd := RootCommand.FindCommand("status")
	if statusCmd == nil {
		t.Error("未找到 status 命令")
	}

	configCmd := RootCommand.FindCommand("config")
	if configCmd == nil {
		t.Error("未找到 config 命令")
	}
}

func TestCommandWithArgs(t *testing.T) {
	root := &Command{
		Name:  "test",
		Short: "测试命令",
	}

	var receivedArgs []string
	root.AddCommand(&Command{
		Name:  "echo",
		Short: "回显参数",
		Args:  -1, // 任意数量参数
		Run: func(args []string) error {
			receivedArgs = args
			return nil
		},
	})

	// 测试传递多个参数
	err := root.Execute([]string{"echo", "hello", "world", "!"})
	if err != nil {
		t.Errorf("执行失败：%v", err)
	}

	if len(receivedArgs) != 3 {
		t.Errorf("期望 3 个参数，得到 %d 个", len(receivedArgs))
	}

	if receivedArgs[0] != "hello" || receivedArgs[1] != "world" || receivedArgs[2] != "!" {
		t.Errorf("参数值不正确：%v", receivedArgs)
	}
}

func TestCommandErrorHandling(t *testing.T) {
	root := &Command{
		Name:  "test",
		Short: "测试命令",
	}

	root.AddCommand(&Command{
		Name:  "fail",
		Short: "失败命令",
		Run: func(args []string) error {
			return &customError{message: "自定义错误"}
		},
	})

	err := root.Execute([]string{"fail"})
	if err == nil {
		t.Error("期望错误，但成功了")
	}

	if !strings.Contains(err.Error(), "自定义错误") {
		t.Errorf("错误消息不包含'自定义错误'：%v", err)
	}
}

type customError struct {
	message string
}

func (e *customError) Error() string {
	return e.message
}

func TestCommandHelpOutput(t *testing.T) {
	// 捕获标准输出
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	root := &Command{
		Name:  "test",
		Short: "测试命令",
		Long:  "这是一个测试命令",
	}

	root.AddCommand(&Command{
		Name:  "cmd1",
		Short: "命令 1",
	})

	root.AddCommand(&Command{
		Name:  "cmd2",
		Short: "命令 2",
	})

	root.PrintHelp()

	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	// 验证输出包含预期内容
	if !strings.Contains(output, "test") {
		t.Error("帮助输出应包含命令名称")
	}

	if !strings.Contains(output, "cmd1") {
		t.Error("帮助输出应包含 cmd1")
	}

	if !strings.Contains(output, "cmd2") {
		t.Error("帮助输出应包含 cmd2")
	}
}

func TestCommandHelpSorting(t *testing.T) {
	root := &Command{
		Name:  "test",
		Short: "测试命令",
	}

	// 添加无序的命令
	root.AddCommand(&Command{
		Name:  "zebra",
		Short: "Z 命令",
	})

	root.AddCommand(&Command{
		Name:  "alpha",
		Short: "A 命令",
	})

	root.AddCommand(&Command{
		Name:  "middle",
		Short: "M 命令",
	})

	// 捕获输出
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	root.PrintHelp()

	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	// 验证命令按字母顺序排列
	alphaPos := strings.Index(output, "alpha")
	middlePos := strings.Index(output, "middle")
	zebraPos := strings.Index(output, "zebra")

	if alphaPos > middlePos || middlePos > zebraPos {
		t.Error("命令应该按字母顺序排列")
	}
}

func TestSubcommandExecution(t *testing.T) {
	root := &Command{
		Name:  "test",
		Short: "测试命令",
	}

	parent := &Command{
		Name:  "parent",
		Short: "父命令",
	}

	executed := false
	parent.AddCommand(&Command{
		Name:  "child",
		Short: "子命令",
		Run: func(args []string) error {
			executed = true
			return nil
		},
	})

	root.AddCommand(parent)

	// 执行子命令
	err := root.Execute([]string{"parent", "child"})
	if err != nil {
		t.Errorf("执行子命令失败：%v", err)
	}

	if !executed {
		t.Error("子命令未被执行")
	}
}

func TestSubcommandWithArgs(t *testing.T) {
	root := &Command{
		Name:  "test",
		Short: "测试命令",
	}

	parent := &Command{
		Name:  "config",
		Short: "配置命令",
	}

	var receivedArgs []string
	parent.AddCommand(&Command{
		Name:  "set",
		Short: "设置配置",
		Args:  2,
		Run: func(args []string) error {
			receivedArgs = args
			return nil
		},
	})

	root.AddCommand(parent)

	// 执行带参数的子命令
	err := root.Execute([]string{"config", "set", "key", "value"})
	if err != nil {
		t.Errorf("执行子命令失败：%v", err)
	}

	if len(receivedArgs) != 2 {
		t.Errorf("期望 2 个参数，得到 %d 个", len(receivedArgs))
	}

	if receivedArgs[0] != "key" || receivedArgs[1] != "value" {
		t.Errorf("参数值不正确：%v", receivedArgs)
	}
}

func TestSubcommandNotFound(t *testing.T) {
	root := &Command{
		Name:  "test",
		Short: "测试命令",
	}

	parent := &Command{
		Name:  "parent",
		Short: "父命令",
	}

	parent.AddCommand(&Command{
		Name:  "child",
		Short: "子命令",
	})

	root.AddCommand(parent)

	// 执行不存在的子命令
	err := root.Execute([]string{"parent", "nonexistent"})
	if err == nil {
		t.Error("期望子命令未找到错误，但成功了")
	}
}

func TestCommandWithFlags(t *testing.T) {
	root := &Command{
		Name:  "test",
		Short: "测试命令",
	}

	flagsCalled := false
	root.AddCommand(&Command{
		Name:  "serve",
		Short: "服务命令",
		Flags: func() {
			flagsCalled = true
		},
		Run: func(args []string) error {
			return nil
		},
	})

	err := root.Execute([]string{"serve"})
	if err != nil {
		t.Errorf("执行失败：%v", err)
	}

	if !flagsCalled {
		t.Error("Flags 函数应该被调用")
	}
}

func TestCommandNoRunFunction(t *testing.T) {
	root := &Command{
		Name:  "test",
		Short: "测试命令",
	}

	// 添加没有 Run 函数的命令
	root.AddCommand(&Command{
		Name:  "norf",
		Short: "无运行函数",
	})

	// 应该不报错，只是不执行任何操作
	err := root.Execute([]string{"norf"})
	if err != nil {
		t.Errorf("没有 Run 函数的命令不应该报错：%v", err)
	}
}

func TestEmptyArgs(t *testing.T) {
	root := &Command{
		Name:  "test",
		Short: "测试命令",
	}

	// 捕获输出
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// 空参数应该显示帮助
	err := root.Execute([]string{})
	if err != nil {
		t.Errorf("空参数不应该报错：%v", err)
	}

	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	if !strings.Contains(output, "使用方法") {
		t.Error("空参数应该显示帮助信息")
	}
}

func TestCommandUsage(t *testing.T) {
	root := &Command{
		Name:  "test",
		Short: "测试命令",
	}

	root.AddCommand(&Command{
		Name:  "serve",
		Short: "服务命令",
		Usage: "[选项]",
	})

	// 捕获输出
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	root.PrintCommandHelp("serve")

	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	if !strings.Contains(output, "使用方法") {
		t.Error("应该显示使用方法")
	}

	if !strings.Contains(output, "[选项]") {
		t.Error("应该显示用法说明")
	}
}

func TestMultipleSubcommands(t *testing.T) {
	root := &Command{
		Name:  "test",
		Short: "测试命令",
	}

	parent := &Command{
		Name:  "config",
		Short: "配置命令",
	}

	parent.AddCommand(&Command{
		Name:  "show",
		Short: "显示配置",
		Run: func(args []string) error {
			return nil
		},
	})

	parent.AddCommand(&Command{
		Name:  "set",
		Short: "设置配置",
		Run: func(args []string) error {
			return nil
		},
	})

	parent.AddCommand(&Command{
		Name:  "delete",
		Short: "删除配置",
		Run: func(args []string) error {
			return nil
		},
	})

	root.AddCommand(parent)

	// 测试所有子命令都能执行
	subcommands := []string{"show", "set", "delete"}
	for _, sub := range subcommands {
		err := root.Execute([]string{"config", sub})
		if err != nil {
			t.Errorf("子命令 %s 执行失败：%v", sub, err)
		}
	}
}

func TestCommandLongDescription(t *testing.T) {
	root := &Command{
		Name:  "test",
		Short: "测试命令",
	}

	root.AddCommand(&Command{
		Name:  "serve",
		Short: "短描述",
		Long:  "这是一个很长的描述，用于详细说明命令的功能",
	})

	// 捕获输出
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	root.PrintCommandHelp("serve")

	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	// 应该显示 Long 而不是 Short
	if strings.Contains(output, "短描述") {
		t.Error("应该显示 Long 描述而不是 Short 描述")
	}

	if !strings.Contains(output, "这是一个很长的描述") {
		t.Error("应该显示 Long 描述")
	}
}
