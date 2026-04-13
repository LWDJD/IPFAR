package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/lwdjd/IPFAR/config"
	"github.com/lwdjd/IPFAR/internal/log"
	"github.com/lwdjd/IPFAR/lang"
)

func init() {
	var err error
	// 先初始化一个最小化的日志系统（用于配置加载过程中的日志）
	if err := log.Init(log.Config{
		Level:    log.INFO,
		FilePath: "", // 暂时不写文件
		UseJSON:  false,
	}); err != nil {
		fmt.Fprintf(os.Stderr, "初始化日志系统失败：%v\n", err)
		os.Exit(1)
	}

	// 加载配置文件
	config.ConfigFile, err = config.GetConfig()
	if err != nil {
		log.Fatal("加载配置文件失败：%v", err)
	}
	log.Info("配置文件加载成功：%s", config.ConfigFile.Language)

	// 重新初始化日志系统（使用配置文件中的设置）
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

	name := flag.String("name", "NULL", config.Loc.Get("name of the IPFAR"))
	flag.Parse()

	fmt.Println(config.Loc.Get("Hello, World!"))
	fmt.Println(config.Loc.Get("My name is %s.", *name))

	log.Info("程序正常退出")
}
