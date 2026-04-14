package main

import (
	"fmt"
	"os"

	"github.com/lwdjd/IPFAR/internal/flags"
)

func main() {
	// 创建参数集合
	fs := flags.NewFlagSet("ipfar-demo")

	// 定义参数
	fs.Define(flags.Flag{
		Name:     "config",
		Short:    "c",
		Value:    "config.json",
		Usage:    "配置文件路径",
		Required: false,
		ValidateFunc: flags.ValidateFilePath,
	})

	fs.DefineString("name", "n", "World", "要问候的名字", false)
	fs.DefineBool("verbose", "v", false, "启用详细输出")
	fs.DefineInt("port", "p", 8080, "服务端口号")

	fs.Define(flags.Flag{
		Name:     "output",
		Short:    "o",
		Value:    "",
		Usage:    "输出文件路径",
		Required: true,
		ValidateFunc: func(value string) error {
			if value == "" {
				return fmt.Errorf("输出文件不能为空")
			}
			return nil
		},
	})

	// 解析参数
	if err := fs.Parse(); err != nil {
		fmt.Fprintf(os.Stderr, "错误：%v\n", err)
		fmt.Println("\n使用 -h 查看帮助信息")
		os.Exit(1)
	}

	// 获取参数值
	configFile := fs.GetString("config")
	name := fs.GetString("name")
	verbose := fs.GetBool("verbose")
	port := fs.GetInt("port")
	output := fs.GetString("output")

	// 使用参数
	fmt.Printf("配置文件：%s\n", configFile)
	fmt.Printf("名字：%s\n", name)
	fmt.Printf("详细模式：%v\n", verbose)
	fmt.Printf("端口：%d\n", port)
	fmt.Printf("输出文件：%s\n", output)

	if verbose {
		fmt.Println("\n[详细] 参数解析成功！")
	}

	if fs.IsSet("config") {
		fmt.Println("\n用户自定义了配置文件")
	} else {
		fmt.Println("\n使用默认配置文件")
	}
}
