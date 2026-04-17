package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/lwdjd/IPFAR/config"
	"github.com/lwdjd/IPFAR/internal/cmd"
	"github.com/lwdjd/IPFAR/internal/log"
	"github.com/lwdjd/IPFAR/lang"
)

func init() {
	var err error

	if err := log.Init(log.Config{
		Level:      log.INFO,
		FilePath:   "",
		UseConsole: false,
		UseJSON:    false,
	}); err != nil {
		fmt.Fprintf(os.Stderr, "初始化日志系统失败：%v\n", err)
		os.Exit(1)
	}

	config.ConfigFile, err = config.GetConfig()
	if err != nil {
		log.Warn("加载配置文件失败：%v，使用内置默认配置", err)
		config.ConfigFile = &config.Config{
			Language:     "en_US",
			LogLevel:     "info",
			LogFile:      "logs/ipfar.log",
			LogToConsole: false,
			LogFormat:    "text",
		}
	}

	if err := config.InitLog(); err != nil {
		log.Fatal("初始化日志系统失败：%v", err)
	}

	config.Loc = lang.GetLocale(config.ConfigFile.Language)
}

func main() {
	defer log.Close()

	log.SetPrefix("IPFAR")

	setupSignalHandler()

	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "错误：%v\n", err)
		os.Exit(1)
	}
}

func setupSignalHandler() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Info("收到信号：%v，正在优雅关闭...", sig)
		log.Close()
		os.Exit(0)
	}()
}
