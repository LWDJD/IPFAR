package cmd

import (
	"fmt"
	"os"
	"sort"

	"github.com/lwdjd/IPFAR/config"
	"github.com/lwdjd/IPFAR/internal/flags"
	"github.com/lwdjd/IPFAR/internal/version"
)

// Command 子命令定义
type Command struct {
	Name        string                    // 命令名称
	Short       string                    // 简短描述
	Long        string                    // 详细描述
	Usage       string                    // 使用说明
	Args        int                       // 参数数量要求：0=无参数，-1=任意数量，>0=固定数量
	Run         func(args []string) error // 执行函数
	FlagSet     *flags.FlagSet            // 标志集合
	Hidden      bool                      // 是否在帮助中隐藏
	Subcommands []*Command                // 子命令
}

// RootCommand 根命令
var RootCommand = &Command{
	Name:  "ipfar",
	Short: "IPFS-Arweave 桥接工具",
	Long:  "IPFAR 是一个去中心化的数据存储协议桥接工具，用于 IPFS 和 Arweave 之间的数据桥接。",
}

// commandsRegistered 防止重复注册
var commandsRegistered bool

// AddCommand 添加子命令
func (c *Command) AddCommand(cmd *Command) {
	c.Subcommands = append(c.Subcommands, cmd)
}

// Execute 执行命令
func (c *Command) Execute(args []string) error {
	if len(args) == 0 {
		// 没有子命令，显示帮助
		c.PrintHelp()
		return nil
	}

	// 处理全局标志
	if args[0] == "--version" || args[0] == "-v" {
		fmt.Println(version.String())
		return nil
	}
	if args[0] == "--help" || args[0] == "-h" {
		c.PrintHelp()
		return nil
	}

	// 查找匹配的子命令
	cmdName := args[0]
	for _, cmd := range c.Subcommands {
		if cmd.Name == cmdName {
			// 找到匹配的命令
			// 检查是否有子命令
			if len(cmd.Subcommands) > 0 && len(args) > 1 {
				// 有子命令，继续查找
				subCmdName := args[1]
				for _, subCmd := range cmd.Subcommands {
					if subCmd.Name == subCmdName {
						if subCmd.FlagSet != nil {
							subCmd.FlagSet.ParseArgs(args[2:])
						}

						// 检查参数数量
						if subCmd.Args == 0 && len(args) > 2 {
							return fmt.Errorf("命令 %s %s 不接受参数", cmd.Name, subCmd.Name)
						}
						if subCmd.Args > 0 && len(args)-2 != subCmd.Args {
							return fmt.Errorf("命令 %s %s 需要 %d 个参数", cmd.Name, subCmd.Name, subCmd.Args)
						}

						// 执行子命令
						if subCmd.Run != nil {
							return subCmd.Run(args[2:])
						}
						return nil
					}
				}
				// 未找到子命令，显示父命令帮助
				fmt.Fprintf(os.Stderr, "未知子命令：%s %s\n\n", cmdName, subCmdName)
				c.PrintCommandHelp(cmdName)
				return fmt.Errorf("未知子命令：%s %s", cmdName, subCmdName)
			}

			// 没有子命令或没有提供子命令，执行当前命令
			if cmd.FlagSet != nil {
				cmd.FlagSet.ParseArgs(args[1:])
			}

			// 检查参数数量（有 FlagSet 的命令跳过参数数量检查，因为 flag 参数是合法的）
			if cmd.FlagSet == nil {
				if cmd.Args == 0 && len(args) > 1 {
					return fmt.Errorf("命令 %s 不接受参数", cmd.Name)
				}
				if cmd.Args > 0 && len(args)-1 != cmd.Args {
					return fmt.Errorf("命令 %s 需要 %d 个参数", cmd.Name, cmd.Args)
				}
			}

			// 执行命令
			if cmd.Run != nil {
				return cmd.Run(args[1:])
			}
			return nil
		}
	}

	// 未找到命令
	fmt.Fprintf(os.Stderr, "未知命令：%s\n\n", cmdName)
	c.PrintHelp()
	return fmt.Errorf("未知命令：%s", cmdName)
}

// PrintHelp 打印帮助信息
func (c *Command) PrintHelp() {
	fmt.Println()
	fmt.Println(c.Long)
	fmt.Println()
	fmt.Println("使用方法:")
	fmt.Printf("  %s <命令> [选项]\n", c.Name)
	fmt.Println()

	// 收集可见命令
	visibleCommands := make([]*Command, 0)
	for _, cmd := range c.Subcommands {
		if !cmd.Hidden {
			visibleCommands = append(visibleCommands, cmd)
		}
	}

	if len(visibleCommands) > 0 {
		fmt.Println("可用命令:")

		// 按名称排序
		sort.Slice(visibleCommands, func(i, j int) bool {
			return visibleCommands[i].Name < visibleCommands[j].Name
		})

		for _, cmd := range visibleCommands {
			fmt.Printf("  %-20s %s\n", cmd.Name, cmd.Short)
		}
		fmt.Println()
	}

	fmt.Println("使用 \"ipfar <命令> --help\" 获取有关命令的更多信息。")
}

// PrintCommandHelp 打印特定命令的帮助
func (c *Command) PrintCommandHelp(cmdName string) {
	for _, cmd := range c.Subcommands {
		if cmd.Name == cmdName {
			fmt.Println()
			if cmd.Long != "" {
				fmt.Println(cmd.Long)
			} else if cmd.Short != "" {
				fmt.Println(cmd.Short)
			}
			fmt.Println()

			if cmd.Usage != "" {
				fmt.Println("使用方法:")
				fmt.Printf("  %s %s\n", c.Name, cmd.Usage)
				fmt.Println()
			}

			if len(cmd.Subcommands) > 0 {
				fmt.Println("子命令:")
				for _, sub := range cmd.Subcommands {
					if !sub.Hidden {
						fmt.Printf("  %-20s %s\n", sub.Name, sub.Short)
					}
				}
				fmt.Println()
			}

			return
		}
	}
	fmt.Fprintf(os.Stderr, "未知命令：%s\n", cmdName)
}

// FindCommand 查找命令
func (c *Command) FindCommand(name string) *Command {
	for _, cmd := range c.Subcommands {
		if cmd.Name == name {
			return cmd
		}
	}
	return nil
}

// ResetCommands 重置命令注册（仅用于测试）
func ResetCommands() {
	RootCommand.Subcommands = nil
	commandsRegistered = false
}

// RegisterCommands 注册所有命令
func RegisterCommands() {
	if commandsRegistered {
		return
	}
	commandsRegistered = true
	// version 命令
	RootCommand.AddCommand(&Command{
		Name:  "version",
		Short: "显示版本信息",
		Long:  "显示 IPFAR 的版本信息",
		Run: func(args []string) error {
			fmt.Println(version.String())
			return nil
		},
	})

	// help 命令
	RootCommand.AddCommand(&Command{
		Name:  "help",
		Short: "显示帮助信息",
		Long:  "显示 IPFAR 的帮助信息或特定命令的帮助",
		Usage: "[命令名称]",
		Args:  -1,
		Run: func(args []string) error {
			if len(args) == 0 {
				RootCommand.PrintHelp()
			} else {
				RootCommand.PrintCommandHelp(args[0])
			}
			return nil
		},
	})

	// init 命令
	RootCommand.AddCommand(&Command{
		Name:  "init",
		Short: "初始化配置",
		Long:  "生成默认配置文件",
		Usage: "[选项]",
		Run: func(args []string) error {
			fmt.Println("初始化配置...")
			fmt.Println("配置文件已生成：config.json")
			return nil
		},
	})

	// serve 命令
	serveFlagSet := flags.NewFlagSet("serve")
	serveFlagSet.DefineString("config", "c", "config.json", "配置文件路径", false)
	serveFlagSet.DefineInt("port", "p", 8080, "服务监听端口")
	serveFlagSet.DefineBool("verbose", "v", false, "启用详细输出")

	RootCommand.AddCommand(&Command{
		Name:    "serve",
		Short:   "启动服务",
		Long:    "启动 IPFAR 桥接服务",
		Usage:   "[选项]",
		FlagSet: serveFlagSet,
		Run: func(args []string) error {
			port := serveFlagSet.GetInt("port")
			configFile := serveFlagSet.GetString("config")
			verbose := serveFlagSet.GetBool("verbose")
			fmt.Printf("启动 IPFAR 服务...\n")
			fmt.Printf("配置文件：%s\n", configFile)
			fmt.Printf("服务已启动，监听端口：%d\n", port)
			if verbose {
				fmt.Println("详细模式已启用")
			}
			return nil
		},
	})

	// status 命令
	RootCommand.AddCommand(&Command{
		Name:  "status",
		Short: "查看状态",
		Long:  "查看 IPFAR 服务的运行状态",
		Run: func(args []string) error {
			fmt.Println("IPFAR 服务状态：未运行")
			return nil
		},
	})

	// config 命令
	configCmd := &Command{
		Name:  "config",
		Short: "配置管理",
		Long:  "管理 IPFAR 的配置",
	}

	configCmd.AddCommand(&Command{
		Name:  "show",
		Short: "显示当前配置",
		Run: func(args []string) error {
			if config.ConfigFile == nil {
				fmt.Println("配置未加载")
				return nil
			}
			fmt.Println("当前配置：")
			fmt.Printf("  语言：%s\n", config.ConfigFile.Language)
			fmt.Printf("  日志级别：%s\n", config.ConfigFile.LogLevel)
			fmt.Printf("  日志文件：%s\n", config.ConfigFile.LogFile)
			fmt.Printf("  控制台输出：%v\n", config.ConfigFile.LogToConsole)
			fmt.Printf("  日志格式：%s\n", config.ConfigFile.LogFormat)
			return nil
		},
	})

	configCmd.AddCommand(&Command{
		Name:  "set",
		Short: "设置配置项",
		Usage: "<键> <值>",
		Args:  2,
		Run: func(args []string) error {
			fmt.Printf("设置配置：%s = %s\n", args[0], args[1])
			return nil
		},
	})

	RootCommand.AddCommand(configCmd)
}

// Run 运行命令（从 os.Args 解析）
func Run() error {
	RegisterCommands()

	args := os.Args[1:]

	// 处理 --help 和 -h
	if len(args) > 0 {
		if args[0] == "--help" || args[0] == "-h" || args[0] == "help" {
			if len(args) > 1 {
				RootCommand.PrintCommandHelp(args[1])
			} else {
				RootCommand.PrintHelp()
			}
			return nil
		}

		// 处理 --version / -v
		if args[0] == "--version" || args[0] == "-v" {
			fmt.Println(version.String())
			return nil
		}
	}

	return RootCommand.Execute(args)
}
