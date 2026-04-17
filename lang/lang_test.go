package lang

import (
	"testing"

	"golang.org/x/text/language"
)

func TestTagToPOSIX(t *testing.T) {
	tests := []struct {
		tag      string
		expected string
	}{
		{"zh-CN", "zh_CN"},
		{"zh-Hans-CN", "zh_CN"},
		{"en-US", "en_US"},
		{"en-GB", "en_GB"},
		{"ja-JP", "ja_JP"},
		{"ko-KR", "ko_KR"},
		{"fr-FR", "fr_FR"},
		{"de-DE", "de_DE"},
	}

	for _, tt := range tests {
		tag := language.Make(tt.tag)
		result := tagToPOSIX(tag)
		if result != tt.expected {
			t.Errorf("tagToPOSIX(%s) = %s, 期望 %s", tt.tag, result, tt.expected)
		}
	}
}

func TestTagToPOSIXBaseOnly(t *testing.T) {
	tag := language.MustParse("en")
	base, _ := tag.Base()
	region, _ := tag.Region()

	if region.String() != "" {
		t.Logf("language.Make(\"en\") 推断了地区 %s，跳过纯语言测试", region.String())
	}

	result := tagToPOSIX(tag)
	if !isValidPOSIXResult(result, base.String()) {
		t.Errorf("tagToPOSIX(en) = %s，应以 en 开头", result)
	}
}

func isValidPOSIXResult(result, base string) bool {
	return result == base || (len(result) > len(base) && result[:len(base)+1] == base+"_")
}

func TestGetSystemLanguage(t *testing.T) {
	lang := GetSystemLanguage()
	if lang == "" {
		t.Error("GetSystemLanguage 不应返回空字符串")
	}
}

func TestGetLocaleWithEmbedded(t *testing.T) {
	loc := GetLocale("en_US")
	if loc == nil {
		t.Error("GetLocale 不应返回 nil")
	}
}

func TestGetLocaleWithInvalidLang(t *testing.T) {
	loc := GetLocale("xx_XX")
	if loc == nil {
		t.Error("GetLocale 即使语言不存在也不应返回 nil")
	}
}

func TestDefaultLang(t *testing.T) {
	if DefaultLang != "en_US" {
		t.Errorf("DefaultLang 应为 en_US，得到 %s", DefaultLang)
	}
}

func TestDefaultDomain(t *testing.T) {
	if DefaultDomain != "main" {
		t.Errorf("DefaultDomain 应为 main，得到 %s", DefaultDomain)
	}
}
