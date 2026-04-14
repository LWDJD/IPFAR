package main

import (
	"fmt"
	"os"

	"github.com/lwdjd/IPFAR/config"
	"github.com/lwdjd/IPFAR/internal/flags"
	"github.com/lwdjd/IPFAR/internal/log"
	"github.com/lwdjd/IPFAR/lang"
)

func init() {
	var err error

	// 先初始化一个最小化的日志系统（用于配置加载过程中的日志）
	// 默认不输出日志，等待配置文件加载后再根据配置初始化
	if err := log.Init(log.Config{
		Level:      log.INFO,
		FilePath:   "",
		UseConsole: false,
		UseJSON:    false,
	}); err != nil {
		fmt.Fprintf(os.Stderr, "初始化日志系统失败：%v\n", err)
		os.Exit(1)
	}

	// 加载配置文件
	config.ConfigFile, err = config.GetConfig()
	if err != nil {
		// 配置文件加载失败，使用默认配置
		log.Warn("加载配置文件失败：%v，使用默认配置", err)
		config.ConfigFile = &config.Config{
			Language:     "zh_CN",
			LogLevel:     "info",
			LogFile:      "logs/ipfar.log",
			LogToConsole: true,
			LogFormat:    "text",
		}
	}

	// 根据配置文件重新初始化日志系统
	if err := config.InitLog(); err != nil {
		log.Fatal("初始化日志系统失败：%v", err)
	}

	// 初始化语言
	config.Loc = lang.GetLocale(config.ConfigFile.Language)
}

func main() {
	// 确保程序退出时关闭日志
	defer log.Close()

	// 设置日志前缀
	log.SetPrefix("IPFAR")

	// 创建参数集合
	fs := flags.NewFlagSet("ipfar")

	// 定义参数
	fs.DefineString("name", "n", "NULL", config.Loc.Get("name of the IPFAR"), false)
	fs.DefineString("config", "c", "config.json", "配置文件路径", false)
	fs.DefineBool("verbose", "v", false, "启用详细输出模式")
	fs.DefineBool("version", "", false, "显示版本号")

	// 解析参数
	if err := fs.Parse(); err != nil {
		fmt.Fprintf(os.Stderr, "错误：%v\n\n", err)
		fmt.Println("使用 -h 查看帮助信息")
		os.Exit(1)
	}

	// 获取参数值
	name := fs.GetString("name")
	configFile := fs.GetString("config")
	verbose := fs.GetBool("verbose")
	version := fs.GetBool("version")

	// 处理 version 参数
	if version {
		fmt.Println("IPFAR v1.0.0")
		log.Info("程序正常退出")
		return
	}

	// 如果用户指定了配置文件，重新加载
	if fs.IsSet("config") {
		log.Info("使用自定义配置文件：%s", configFile)
		// 这里可以添加重新加载配置的逻辑
	}

	// 输出信息
	fmt.Println(config.Loc.Get("Hello, World!"))
	fmt.Println(config.Loc.Get("My name is %s.", name))

	if verbose {
		log.Info("详细模式已启用")
		log.Info("配置文件：%s", configFile)
		log.Info("语言：%s", config.ConfigFile.Language)
	}

	log.Info("程序正常退出")
}
