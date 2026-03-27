package main

import (
	"flag"
	"fmt"

	"github.com/lwdjd/IPFAR/config"
	"github.com/lwdjd/IPFAR/lang"
)

func init() {
	err := error(nil)
	// 加载配置文件
	config.ConfigFile, err = config.GetConfig()
	if err != nil {
		panic(err)
	}
	// 初始化语言
	config.Loc = lang.GetLocale(config.ConfigFile.Language)
}
func main() {

	name := flag.String("name", "NULL", config.Loc.Get("name of the IPFAR"))
	flag.Parse()

	fmt.Println(config.Loc.Get("Hello, World!"))
	fmt.Println(config.Loc.Get("My name is %s.", *name))
}
