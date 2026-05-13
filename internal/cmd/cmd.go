package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"

	"github.com/lwdjd/IPFAR/config"
	"github.com/lwdjd/IPFAR/internal/bridge"
	"github.com/lwdjd/IPFAR/internal/flags"
	"github.com/lwdjd/IPFAR/internal/version"

	sdkmeta "github.com/LWDJD/ipfar-sdk/verify/metadata"
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

	// verify 命令组
	verifyCmd := &Command{
		Name:  "verify",
		Short: "验证工具",
		Long:  "IPFAR 数据验证工具集",
	}

	// verify metadata 子命令
	verifyMetadataFlagSet := flags.NewFlagSet("verify-metadata")
	verifyMetadataFlagSet.DefineBool("json", "j", false, "输入为原始 JSON（默认：Base64URL 编码）")

	verifyCmd.AddCommand(&Command{
		Name:    "metadata",
		Short:   "验证元数据",
		Long:    "验证 IPFAR 元数据的合法性和完整性",
		Usage:   "<元数据字符串>",
		Args:    1,
		FlagSet: verifyMetadataFlagSet,
		Run: func(args []string) error {
			isJSON := verifyMetadataFlagSet.GetBool("json")

			verifyPoW, verifyIdx, verifyRef, verifyIntegrity := config.ConfigFile.GetVerifyConfig()
			b := bridge.NewBridge(verifyPoW, verifyIdx, verifyRef, verifyIntegrity)

			var meta *sdkmeta.Metadata
			var err error

			if isJSON {
				meta, err = b.VerifyMetadata([]byte(args[0]))
			} else {
				meta, err = b.VerifyMetadataFromBase64(args[0])
			}

			if err != nil {
				return err
			}

			// 运行验证管道
			result := b.RunPipeline(meta, false)
			fmt.Print(bridge.PrintPipelineResult(result))

			// 显示解析后的元数据
			formatted, _ := bridge.FormatMetadataJSON(meta)
			fmt.Println("\n解析后的元数据：")
			fmt.Println(formatted)

			return nil
		},
	})

	// verify pow 子命令
	verifyPoWFlagSet := flags.NewFlagSet("verify-pow")
	verifyPoWFlagSet.DefineString("root-cid", "r", "", "根 CID（Base32）", true)
	verifyPoWFlagSet.DefineString("data-txid", "t", "", "数据交易 ID", true)
	verifyPoWFlagSet.DefineString("data-size", "s", "", "原始数据大小（字节）", true)
	verifyPoWFlagSet.DefineString("pow", "p", "", "PoW salt 值", false)
	verifyPoWFlagSet.DefineString("pow-alg", "a", "argon2id-light-v1", "PoW 算法标识", false)

	verifyCmd.AddCommand(&Command{
		Name:    "pow",
		Short:   "验证工作量证明",
		Long:    "验证 Argon2id PoW 是否满足难度要求",
		Usage:   "[选项]",
		FlagSet: verifyPoWFlagSet,
		Run: func(args []string) error {
			rootCID := verifyPoWFlagSet.GetString("root-cid")
			dataTXID := verifyPoWFlagSet.GetString("data-txid")
			dataSizeStr := verifyPoWFlagSet.GetString("data-size")
			powStr := verifyPoWFlagSet.GetString("pow")
			powAlg := verifyPoWFlagSet.GetString("pow-alg")

			if rootCID == "" || dataTXID == "" || dataSizeStr == "" {
				return fmt.Errorf("必须提供 --root-cid, --data-txid, --data-size 参数")
			}

			dataSize, err := strconv.ParseInt(dataSizeStr, 10, 64)
			if err != nil {
				return fmt.Errorf("无效的 data-size: %v", err)
			}

			verifyPoW, _, _, _ := config.ConfigFile.GetVerifyConfig()
			b := bridge.NewBridge(verifyPoW, false, false, false)

			meta := &sdkmeta.Metadata{
				RootCID:  rootCID,
				DataTXID: dataTXID,
				DataSize: int(dataSize),
				PoW:      powStr,
				PoWAlg:   powAlg,
			}

			if err := b.VerifyPoW(meta); err != nil {
				return err
			}

			fmt.Println("✅ PoW 验证通过")
			if dataSize >= 100*1024*1024 {
				fmt.Println("（文件大小 >= 100 MiB，免 PoW 验证）")
			} else {
				fmt.Printf("   Root CID: %s\n", rootCID)

				fmt.Printf("   Data TXID: %s\n", dataTXID)

				fmt.Printf("   PoW salt: %s\n", powStr)

			}

			return nil
		},
	})

	// verify config 子命令
	verifyCmd.AddCommand(&Command{
		Name:  "config",
		Short: "显示验证配置",
		Long:  "显示当前的验证选项配置",
		Run: func(args []string) error {
			verifyPoW, verifyIdx, verifyRef, verifyIntegrity := config.ConfigFile.GetVerifyConfig()

			fmt.Println("当前验证配置：")
			fmt.Printf("  verify_pow:             %v\n", verifyPoW)

			fmt.Printf("  verify_index:           %v\n", verifyIdx)

			fmt.Printf("  verify_reference_chain: %v\n", verifyRef)

			fmt.Printf("  verify_integrity:        %v\n", verifyIntegrity)


			if config.ConfigFile.SecurityPreset != "" {
				fmt.Printf("  security_preset:        %s\n", config.ConfigFile.SecurityPreset)

			}

			// 显示 PoW 参数
			info := bridge.GetPoWInfo()
			fmt.Println()
			fmt.Println("PoW 参数：")
			fmt.Printf("  算法：%s\n", info.Algorithm)

			fmt.Printf("  内存：%s\n", info.Memory)

			fmt.Printf("  难度：%d 字节前导零\n", info.MinLeadingZeroBytes)

			fmt.Printf("  阈值：%s\n", info.Threshold)


			return nil
		},
	})

	// verify tags 子命令
	verifyCmd.AddCommand(&Command{
		Name:  "tags",
		Short: "验证 Transaction Tags",
		Long:  "验证 Arweave Transaction Tags 是否符合 IPFAR 规范",
		Usage: "<tag_json>",
		Args:  1,
		Run: func(args []string) error {
			tags, err := sdkmeta.ParseJSON([]byte(args[0]))
			if err != nil {
				// 尝试作为 tags 数组解析
				return verifyTagsFromJSON(args[0])
			}

			_ = tags
			return verifyTagsFromJSON(args[0])
		},
	})

	RootCommand.AddCommand(verifyCmd)
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

// verifyTagsFromJSON 从 JSON 字符串解析并验证 Tags
func verifyTagsFromJSON(tagJSON string) error {
	var tags []sdkmeta.Tag
	if err := json.Unmarshal([]byte(tagJSON), &tags); err != nil {
		return fmt.Errorf("无法解析 Tags JSON: %v。\n预期格式：[{\"name\":\"TagName\",\"value\":\"TagValue\"}, ...]", err)
	}

	if err := sdkmeta.ValidateTags(tags); err != nil {
		return fmt.Errorf("Tags 验证失败：%v", err)
	}

	fmt.Println("✅ Tags 验证通过")
	fmt.Println()
	for _, tag := range tags {
		fmt.Printf("  %-25s = %s\n", tag.Name, tag.Value)

	}

	return nil
}
