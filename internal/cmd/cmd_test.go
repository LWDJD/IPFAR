package cmd

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/lwdjd/IPFAR/internal/flags"
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
	ResetCommands()
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

	testFlagSet := flags.NewFlagSet("serve")
	testFlagSet.DefineString("config", "c", "config.json", "配置文件路径", false)
	testFlagSet.DefineInt("port", "p", 8080, "端口")

	root.AddCommand(&Command{
		Name:    "serve",
		Short:   "服务命令",
		FlagSet: testFlagSet,
		Run: func(args []string) error {
			return nil
		},
	})

	err := root.Execute([]string{"serve"})
	if err != nil {
		t.Errorf("执行失败：%v", err)
	}
}

func TestCommandWithFlagParsing(t *testing.T) {
	root := &Command{
		Name:  "test",
		Short: "测试命令",
	}

	testFlagSet := flags.NewFlagSet("serve")
	testFlagSet.DefineInt("port", "p", 8080, "端口")

	var receivedPort int
	root.AddCommand(&Command{
		Name:    "serve",
		Short:   "服务命令",
		FlagSet: testFlagSet,
		Run: func(args []string) error {
			receivedPort = testFlagSet.GetInt("port")
			return nil
		},
	})

	err := root.Execute([]string{"serve", "-port", "9090"})
	if err != nil {
		t.Errorf("执行失败：%v", err)
	}

	if receivedPort != 9090 {
		t.Errorf("期望 port=9090, 得到 port=%d", receivedPort)
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

func TestRegisterCommandsIdempotent(t *testing.T) {
	ResetCommands()
	RegisterCommands()

	countBefore := len(RootCommand.Subcommands)

	RegisterCommands()

	countAfter := len(RootCommand.Subcommands)

	if countBefore != countAfter {
		t.Errorf("重复注册后命令数量变化：之前 %d，之后 %d", countBefore, countAfter)
	}
}

func TestVersionCommand(t *testing.T) {
	ResetCommands()
	RegisterCommands()

	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	err := RootCommand.Execute([]string{"version"})

	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	if err != nil {
		t.Errorf("version 命令执行失败：%v", err)
	}

	if !strings.Contains(output, "IPFAR") {
		t.Error("version 输出应包含 IPFAR")
	}
}

func TestVersionFlag(t *testing.T) {
	ResetCommands()
	RegisterCommands()

	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	err := RootCommand.Execute([]string{"--version"})

	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	if err != nil {
		t.Errorf("--version 执行失败：%v", err)
	}

	if !strings.Contains(output, "IPFAR") {
		t.Error("--version 输出应包含 IPFAR")
	}
}

// TestFlagSetArgsFiltering_Bug1 测试 Bug #1：FlagSet 解析后剩余参数正确过滤
func TestFlagSetArgsFiltering_Bug1(t *testing.T) {
	// 场景1：子命令 + FlagSet + 位置参数
	// 例如: ipfar verify metadata -j <data>
	// 修复前：len(args) 校验使用原始 args（含 flags），导致参数数量判断错误
	// 修复后：使用 flagSet.Args() 进行判断，且 Run 接收过滤后的参数

	root := &Command{Name: "test", Short: "测试"}

	parent := &Command{Name: "verify", Short: "验证"}

	metaFlagSet := flags.NewFlagSet("verify-metadata")
	metaFlagSet.DefineBool("json", "j", false, "JSON 模式")

	var receivedArgs []string
	var jsonFlag bool
	parent.AddCommand(&Command{
		Name:    "metadata",
		Short:   "验证元数据",
		Args:    1,
		FlagSet: metaFlagSet,
		Run: func(args []string) error {
			receivedArgs = args
			jsonFlag = metaFlagSet.GetBool("json")
			return nil
		},
	})

	root.AddCommand(parent)

	// 执行: verify metadata -j mydata
	err := root.Execute([]string{"verify", "metadata", "-j", "mydata"})
	if err != nil {
		t.Fatalf("执行失败：%v", err)
	}

	if len(receivedArgs) != 1 {
		t.Errorf("期望 1 个剩余参数，得到 %d 个：%v", len(receivedArgs), receivedArgs)
	}
	if receivedArgs[0] != "mydata" {
		t.Errorf("期望参数为 'mydata'，得到 '%s'", receivedArgs[0])
	}
	if !jsonFlag {
		t.Error("期望 -j 标志为 true")
	}
}

// TestFlagSetArgsFilteringTopLevel_Bug1 测试 Bug #1：顶层命令 + FlagSet + 位置参数
func TestFlagSetArgsFilteringTopLevel_Bug1(t *testing.T) {
	// 场景2：顶层命令 + FlagSet + 位置参数
	// 例如: ipfar run <txid> --preset strict

	root := &Command{Name: "test", Short: "测试"}

	runFlagSet := flags.NewFlagSet("run")
	runFlagSet.DefineString("preset", "", "light", "预设", false)

	var receivedArgs []string
	var presetValue string
	root.AddCommand(&Command{
		Name:    "run",
		Short:   "运行",
		Args:    1,
		FlagSet: runFlagSet,
		Run: func(args []string) error {
			receivedArgs = args
			presetValue = runFlagSet.GetString("preset")
			return nil
		},
	})

	// 执行: run --preset strict mytxid（flags 必须在位置参数之前）
	err := root.Execute([]string{"run", "--preset", "strict", "mytxid"})
	if err != nil {
		t.Fatalf("执行失败：%v", err)
	}

	if len(receivedArgs) != 1 {
		t.Errorf("期望 1 个剩余参数，得到 %d 个：%v", len(receivedArgs), receivedArgs)
	}
	if receivedArgs[0] != "mytxid" {
		t.Errorf("期望参数为 'mytxid'，得到 '%s'", receivedArgs[0])
	}
	if presetValue != "strict" {
		t.Errorf("期望 preset=strict，得到 preset=%s", presetValue)
	}
}

// TestFlagOnlyCommand_Bug1 测试 Bug #1：纯 FlagSet 命令（无位置参数）
func TestFlagOnlyCommand_Bug1(t *testing.T) {
	// 场景3：只有 FlagSet 没有位置参数的命令
	// 例如: ipfar verify pow --root-cid x --data-txid y --data-size 100
	// 修复前：Args=0 但 len(args)>0 导致 "不接受参数" 错误

	root := &Command{Name: "test", Short: "测试"}

	parent := &Command{Name: "verify", Short: "验证"}

	powFlagSet := flags.NewFlagSet("verify-pow")
	powFlagSet.DefineString("root-cid", "r", "", "根 CID", true)
	powFlagSet.DefineString("data-txid", "t", "", "数据 TXID", true)
	powFlagSet.DefineString("data-size", "s", "", "数据大小", true)

	executed := false
	parent.AddCommand(&Command{
		Name:    "pow",
		Short:   "验证 PoW",
		FlagSet: powFlagSet,
		Run: func(args []string) error {
			executed = true
			return nil
		},
	})

	root.AddCommand(parent)

	// 执行: verify pow --root-cid abc --data-txid xyz --data-size 100
	err := root.Execute([]string{"verify", "pow",
		"--root-cid", "abc",
		"--data-txid", "xyz",
		"--data-size", "100",
	})
	if err != nil {
		t.Fatalf("纯 flag 命令执行失败：%v", err)
	}
	if !executed {
		t.Error("纯 flag 命令的 Run 应该被执行")
	}
}

// TestFlagSetHelpStopsExecution_Bug2 测试 Bug #2：--help 阻止 Run 执行
func TestFlagSetHelpStopsExecution_Bug2(t *testing.T) {
	// 修复前：--help 显示帮助后 Run 仍然执行（如 serve --help 仍启动服务）
	// 修复后：ParseArgs 返回 ErrHelpShown，Execute 检查并返回 nil，不执行 Run

	root := &Command{Name: "test", Short: "测试"}

	testFlagSet := flags.NewFlagSet("serve")
	testFlagSet.DefineInt("port", "p", 8080, "端口")

	runExecuted := false
	root.AddCommand(&Command{
		Name:    "serve",
		Short:   "服务",
		FlagSet: testFlagSet,
		Run: func(args []string) error {
			runExecuted = true
			return nil
		},
	})

	// 执行: serve --help
	oldStdout := os.Stdout
	_, w, _ := os.Pipe()
	os.Stdout = w

	err := root.Execute([]string{"serve", "--help"})

	w.Close()
	os.Stdout = oldStdout

	if err != nil {
		t.Errorf("--help 不应返回错误：%v", err)
	}
	if runExecuted {
		t.Error("--help 后 Run 不应该被执行！")
	}
}

// TestSubcommandFlagSetHelpStopsExecution_Bug2 测试 Bug #2：子命令 --help 阻止 Run 执行
func TestSubcommandFlagSetHelpStopsExecution_Bug2(t *testing.T) {
	root := &Command{Name: "test", Short: "测试"}

	parent := &Command{Name: "verify", Short: "验证"}

	testFlagSet := flags.NewFlagSet("test-flags")
	testFlagSet.DefineBool("json", "j", false, "JSON 模式")

	runExecuted := false
	parent.AddCommand(&Command{
		Name:    "check",
		Short:   "检查",
		FlagSet: testFlagSet,
		Args:    1,
		Run: func(args []string) error {
			runExecuted = true
			return nil
		},
	})

	root.AddCommand(parent)

	// 执行: verify check --help
	oldStdout := os.Stdout
	_, w, _ := os.Pipe()
	os.Stdout = w

	err := root.Execute([]string{"verify", "check", "--help"})

	w.Close()
	os.Stdout = oldStdout

	if err != nil {
		t.Errorf("子命令 --help 不应返回错误：%v", err)
	}
	if runExecuted {
		t.Error("子命令 --help 后 Run 不应该被执行！")
	}
}

// TestFlagSetArgsRemainAfterParsing 测试 FlagSet.Args() 返回正确的剩余参数
func TestFlagSetArgsRemainAfterParsing(t *testing.T) {
	fs := flags.NewFlagSet("test")
	fs.DefineBool("verbose", "v", false, "详细输出")
	fs.DefineString("output", "o", "", "输出文件", false)

	err := fs.ParseArgs([]string{"-v", "--output", "out.txt", "file1", "file2"})
	if err != nil {
		t.Fatalf("ParseArgs 失败：%v", err)
	}

	args := fs.Args()
	if len(args) != 2 {
		t.Errorf("期望 2 个剩余参数，得到 %d 个：%v", len(args), args)
	}
	if args[0] != "file1" || args[1] != "file2" {
		t.Errorf("剩余参数不正确：%v", args)
	}
	if fs.NArg() != 2 {
		t.Errorf("NArg() 期望 2，得到 %d", fs.NArg())
	}
}
