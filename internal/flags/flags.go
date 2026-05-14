package flags

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
)

// ErrHelpShown 表示已显示帮助信息，调用者应停止执行
var ErrHelpShown = errors.New("help shown")

// Flag 定义单个命令行参数
type Flag struct {
	Name         string             // 参数名称
	Short        string             // 短名称（可选）
	Value        string             // 默认值
	Usage        string             // 使用说明
	Required     bool               // 是否必需
	ValidateFunc func(string) error // 验证函数（可选）
}

// FlagSet 命令行参数集合
type FlagSet struct {
	flags     map[string]*Flag
	values    map[string]*string
	flagSet   *flag.FlagSet
	helpShown bool
}

// NewFlagSet 创建新的参数集合
func NewFlagSet(name string) *FlagSet {
	fs := &FlagSet{
		flags:  make(map[string]*Flag),
		values: make(map[string]*string),
	}
	fs.flagSet = flag.NewFlagSet(name, flag.ExitOnError)

	// 自定义帮助信息
	fs.flagSet.Usage = func() {
		fs.ShowHelp()
	}

	return fs
}

// Define 定义一个参数
func (fs *FlagSet) Define(f Flag) {
	fs.flags[f.Name] = &f

	// 构建使用文本
	usageText := f.Usage
	if f.Required {
		usageText = "[必需] " + usageText
	}
	if f.DefaultValue() != "" {
		usageText += fmt.Sprintf(" (默认值：%s)", f.DefaultValue())
	}

	// 注册参数
	value := fs.flagSet.String(f.Name, f.Value, usageText)
	fs.values[f.Name] = value

	// 如果有短名称，注册一个别名参数
	if f.Short != "" {
		fs.flagSet.StringVar(value, f.Short, "", "")
	}
}

// DefineString 快速定义字符串参数
func (fs *FlagSet) DefineString(name, short, defaultValue, usage string, required bool) {
	fs.Define(Flag{
		Name:     name,
		Short:    short,
		Value:    defaultValue,
		Usage:    usage,
		Required: required,
	})
}

// DefineBool 定义布尔参数
func (fs *FlagSet) DefineBool(name, short string, defaultValue bool, usage string) {
	value := fs.flagSet.Bool(name, defaultValue, usage)
	fs.values[name] = nil // 特殊处理

	// 创建包装器
	fs.flags[name] = &Flag{
		Name:  name,
		Short: short,
		Value: fmt.Sprintf("%v", defaultValue),
		Usage: usage,
	}

	if short != "" {
		fs.flagSet.BoolVar(value, short, defaultValue, "")
	}
}

// DefineInt 定义整数参数
func (fs *FlagSet) DefineInt(name, short string, defaultValue int, usage string) {
	value := fs.flagSet.Int(name, defaultValue, usage)
	fs.values[name] = nil

	fs.flags[name] = &Flag{
		Name:  name,
		Short: short,
		Value: fmt.Sprintf("%d", defaultValue),
		Usage: usage,
	}

	if short != "" {
		fs.flagSet.IntVar(value, short, defaultValue, "")
	}
}

// Parse 解析命令行参数（从 os.Args）
func (fs *FlagSet) Parse() error {
	return fs.ParseArgs(os.Args[1:])
}

// ParseArgs 从指定参数列表解析
func (fs *FlagSet) ParseArgs(args []string) error {
	// 检查 --help / -h 标志（在任何解析之前）
	for _, arg := range args {
		if arg == "-h" || arg == "--help" || arg == "-help" {
			fs.ShowHelp()
			return ErrHelpShown
		}
	}

	if err := fs.flagSet.Parse(args); err != nil {
		return fmt.Errorf("解析参数失败：%w", err)
	}

	for name, f := range fs.flags {
		if f.Required {
			if !fs.IsSet(name) {
				return fmt.Errorf("缺少必需参数：-%s", name)
			}
		}

		if f.ValidateFunc != nil {
			value := fs.Get(name)
			if err := f.ValidateFunc(value); err != nil {
				return fmt.Errorf("参数 -%s 验证失败：%w", name, err)
			}
		}
	}

	return nil
}

// Get 获取参数值
func (fs *FlagSet) Get(name string) string {
	if value, ok := fs.values[name]; ok && value != nil {
		return *value
	}
	return ""
}

// GetString 获取字符串参数值
func (fs *FlagSet) GetString(name string) string {
	return fs.Get(name)
}

// GetBool 获取布尔参数值
func (fs *FlagSet) GetBool(name string) bool {
	// 首先尝试从 flag.FlagSet 获取
	f := fs.flagSet.Lookup(name)
	if f != nil {
		return f.Value.String() == "true"
	}

	// 回退到旧方法
	value := fs.Get(name)
	return value == "true" || value == "1" || value == "yes"
}

// GetInt 获取整数参数值
func (fs *FlagSet) GetInt(name string) int {
	// 从 flag.FlagSet 中直接获取
	f := fs.flagSet.Lookup(name)
	if f == nil {
		return 0
	}
	var result int
	fmt.Sscanf(f.Value.String(), "%d", &result)
	return result
}

// Args 返回解析后剩余的非 flag 参数
func (fs *FlagSet) Args() []string {
	return fs.flagSet.Args()
}

// NArg 返回解析后剩余的非 flag 参数数量
func (fs *FlagSet) NArg() int {
	return fs.flagSet.NArg()
}

// ShowHelp 显示帮助信息
func (fs *FlagSet) ShowHelp() {
	if fs.helpShown {
		return
	}
	fs.helpShown = true

	fmt.Println()
	fmt.Println("使用方法:")
	fmt.Printf("  %s [选项]\n", os.Args[0])
	fmt.Println()

	// 分类显示参数
	required := make([]*Flag, 0)
	optional := make([]*Flag, 0)

	for _, f := range fs.flags {
		if f.Required {
			required = append(required, f)
		} else {
			optional = append(optional, f)
		}
	}

	// 显示必需参数
	if len(required) > 0 {
		fmt.Println("必需参数:")
		for _, f := range required {
			fs.printFlag(f)
		}
		fmt.Println()
	}

	// 显示可选参数
	if len(optional) > 0 {
		fmt.Println("可选参数:")
		for _, f := range optional {
			fs.printFlag(f)
		}
		fmt.Println()
	}

	// 显示示例
	fs.showExamples()
}

func (fs *FlagSet) printFlag(f *Flag) {
	nameStr := fmt.Sprintf("  -%s", f.Name)
	if f.Short != "" {
		nameStr += fmt.Sprintf(", -%s", f.Short)
	}

	fmt.Printf("%-30s %s\n", nameStr, f.Usage)

	if f.Required {
		fmt.Printf("%-30s 此参数为必需\n", "")
	}

	defaultVal := f.DefaultValue()
	if defaultVal != "" && !f.Required {
		fmt.Printf("%-30s 默认值：%s\n", "", defaultVal)
	}
}

func (fs *FlagSet) showExamples() {
	fmt.Println("示例:")

	// 根据定义的参数生成示例
	example := fmt.Sprintf("  %s", os.Args[0])
	hasArgs := false

	for _, f := range fs.flags {
		if f.Required {
			value := f.Value
			if value == "" {
				value = "value"
			}
			example += fmt.Sprintf(" -%s %s", f.Name, value)
			hasArgs = true
		}
	}

	if hasArgs {
		fmt.Println(example)
	}

	// 显示一个带可选参数的示例
	optionalExample := fmt.Sprintf("  %s", os.Args[0])
	for _, f := range fs.flags {
		if f.Required {
			value := f.Value
			if value == "" {
				value = "value"
			}
			optionalExample += fmt.Sprintf(" -%s %s", f.Name, value)
		} else if f.Name != "help" {
			optionalExample += fmt.Sprintf(" -%s %s", f.Name, f.Value)
		}
	}

	if len(fs.flags) > 0 {
		fmt.Println(optionalExample)
	}
}

// IsSet 检查参数是否被设置
func (fs *FlagSet) IsSet(name string) bool {
	set := false
	fs.flagSet.Visit(func(f *flag.Flag) {
		if f.Name == name {
			set = true
		}
	})
	return set
}

// DefaultValue 获取默认值
func (f *Flag) DefaultValue() string {
	if f.Value == "" {
		return ""
	}
	return f.Value
}

// ValidateNotEmpty 验证函数：值不能为空
func ValidateNotEmpty(value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("值不能为空")
	}
	return nil
}

// ValidateFilePath 验证函数：验证文件路径
func ValidateFilePath(value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("文件路径不能为空")
	}
	if _, err := os.Stat(value); os.IsNotExist(err) {
		return fmt.Errorf("文件不存在：%s", value)
	}
	return nil
}

// ValidateDirPath 验证函数：验证目录路径
func ValidateDirPath(value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("目录路径不能为空")
	}
	if _, err := os.Stat(value); os.IsNotExist(err) {
		return fmt.Errorf("目录不存在：%s", value)
	}
	info, err := os.Stat(value)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("路径不是目录：%s", value)
	}
	return nil
}

// ValidateNumber 验证函数：验证数字范围
func ValidateNumber(min, max int) func(string) error {
	return func(value string) error {
		var num int
		if _, err := fmt.Sscanf(value, "%d", &num); err != nil {
			return fmt.Errorf("必须是数字")
		}
		if num < min || num > max {
			return fmt.Errorf("必须在 %d 到 %d 之间", min, max)
		}
		return nil
	}
}
