package main

import (
	"fmt"
	"os"

	"github.com/lwdjd/IPFAR/config"
	"github.com/lwdjd/IPFAR/internal/cmd"
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
		// 配置文件加载失败，使用与内嵌默认配置一致的配置
		log.Warn("加载配置文件失败：%v，使用内置默认配置", err)
		config.ConfigFile = &config.Config{
			Language:     "en_US",
			LogLevel:     "info",
			LogFile:      "logs/ipfar.log",
			LogToConsole: false,
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

	// 执行命令
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "错误：%v\n", err)
		os.Exit(1)
	}
}
