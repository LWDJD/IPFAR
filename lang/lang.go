package lang

import (
	"embed"
	"os"
	"path/filepath"

	"github.com/Xuanwo/go-locale"
	"github.com/leonelquinteros/gotext"
	"golang.org/x/text/language"
)

//go:embed language
var localesFS embed.FS

const (
	DefaultLang    = "en_US"    // 默认语言
	LocalesDirName = "language" // 本地和嵌入目录的统一名称
	DefaultDomain  = "main"     // 默认翻译域
)

// GetSystemLanguage 获取当前系统语言，并格式化为 POSIX 形式 (如 zh_CN, en_US)
// 如果获取失败或解析错误，返回默认语言 en_US
func GetSystemLanguage() string {
	// 1. 使用 go-locale 库获取系统语言标签 (BCP 47 格式，如 zh-CN)
	tag, err := locale.Detect()
	if err != nil {
		return DefaultLang
	}

	// 2. 将 BCP 47 标签转换为 POSIX 格式 (如 zh-CN -> zh_CN)
	return tagToPOSIX(tag)
}

// GetLocale 获取 gotext.Locale 对象
// 逻辑：优先使用本地文件系统目录（如果存在且包含对应语言），否则使用嵌入的二进制目录
func GetLocale(defaultLanguage string) *gotext.Locale {
	lang := defaultLanguage
	localPath := LocalesDirName

	// 1. 检查本地是否存在 locales 根目录
	if info, err := os.Stat(localPath); err == nil && info.IsDir() {
		// 2. 检查本地是否存在当前语言的目录 (如 ./language/zh_CN)
		langDir := filepath.Join(localPath, lang)
		if info, err := os.Stat(langDir); err == nil && info.IsDir() {
			// 本地目录存在且包含该语言 -> 使用本地文件系统
			// 注意：gotext.NewLocale 会自动处理 LC_MESSAGES 子目录
			loc := gotext.NewLocale(localPath, lang)
			loc.AddDomain(DefaultDomain)
			return loc
		}
	}

	// 3. 本地不存在或缺少对应语言 -> 使用嵌入的二进制目录
	// NewLocaleFSWithPath 会将 embed.FS 作为文件系统查找
	loc := gotext.NewLocaleFSWithPath(lang, localesFS, LocalesDirName)
	loc.AddDomain(DefaultDomain)
	return loc
}

// tagToPOSIX 将 language.Tag (BCP 47) 转换为 POSIX 格式字符串
// 例如: zh-Hans-CN -> zh_CN, en-US -> en_US
func tagToPOSIX(tag language.Tag) string {
	// 获取基础语言代码 (如 zh, en)
	base, _ := tag.Base()
	baseStr := base.String()

	// 获取地区代码 (如 CN, US)
	region, _ := tag.Region()
	regionStr := region.String()

	// 如果没有地区信息，只返回语言 (如 "en")
	if regionStr == "" {
		return baseStr
	}

	// 组合为 POSIX 格式 (如 "zh_CN")
	return baseStr + "_" + regionStr
}
