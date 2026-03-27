package main

import (
	"flag"
	"fmt"

	"github.com/leonelquinteros/gotext"
	"github.com/lwdjd/IPFAR/config"
	"github.com/lwdjd/IPFAR/lang"
)

var Loc *gotext.Locale

func init() {
	err := error(nil)
	// 加载配置文件
	config.ConfigFile, err = config.GetConfig()
	if err != nil {
		panic(err)
	}
	// 初始化语言
	Loc = lang.GetLocale(config.ConfigFile.Language)
}
func main() {

	name := flag.String("name", "NULL", Loc.Get("name of the IPFAR"))
	flag.Parse()

	fmt.Println(Loc.Get("Hello, World!"))
	fmt.Println(Loc.Get("My name is %s.", *name))
}
