package log

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Level 日志级别
type Level int

const (
	DEBUG Level = iota
	INFO
	WARN
	ERROR
	FATAL
)

// LogEntry 日志条目（用于 JSON 格式）
type LogEntry struct {
	Timestamp string `json:"timestamp"`
	Level     string `json:"level"`
	Prefix    string `json:"prefix,omitempty"`
	Message   string `json:"message"`
}

// Logger 日志记录器
type Logger struct {
	mu          sync.Mutex
	level       Level
	prefix      string
	filePath    string
	file        *os.File
	writer      *bufio.Writer
	useFile     bool
	useConsole  bool // 是否输出到控制台
	useJSON     bool
	encoding    string // "utf-8" or "gbk"
	closeChan   chan struct{}
	flushTicker *time.Ticker
}

// 全局日志实例
var globalLogger *Logger

// Config 日志配置
type Config struct {
	Level         Level         // 日志级别
	FilePath      string        // 日志文件路径（可选）
	UseConsole    bool          // 是否输出到控制台
	UseJSON       bool          // 是否使用 JSON 格式
	Encoding      string        // 编码：utf-8, gbk
	FlushInterval time.Duration // 刷盘间隔
	MaxFiles      int           // 最大保留日志文件数（默认 5）
}

// Init 初始化全局日志
func Init(cfg Config) error {
	// 如果已经初始化过，先关闭旧的日志
	if globalLogger != nil {
		Close()
	}

	globalLogger = &Logger{
		level:      cfg.Level,
		prefix:     "",
		filePath:   cfg.FilePath,
		useConsole: cfg.UseConsole,
		useJSON:    cfg.UseJSON,
		encoding:   cfg.Encoding,
		closeChan:  make(chan struct{}),
	}

	if cfg.FlushInterval == 0 {
		cfg.FlushInterval = 5 * time.Second
	}

	if cfg.MaxFiles <= 0 {
		cfg.MaxFiles = 5
	}

	// 如果指定了文件路径，打开文件
	if cfg.FilePath != "" {
		if err := globalLogger.openFile(); err != nil {
			return fmt.Errorf("打开日志文件失败：%w", err)
		}
		globalLogger.useFile = true

		// 清理旧日志文件
		globalLogger.cleanupOldLogs(cfg.MaxFiles)

		// 启动定时刷盘
		globalLogger.flushTicker = time.NewTicker(cfg.FlushInterval)
		go globalLogger.flushLoop()
	}

	return nil
}

// openFile 打开日志文件
func (l *Logger) openFile() error {
	// 生成带日期时间和序号的日志文件名
	filePath := l.generateLogFilePath()
	l.filePath = filePath

	// 创建目录
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	// 打开文件（写入模式，每次启动新建）
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	// 写入 UTF-8 BOM
	f.Write([]byte{0xEF, 0xBB, 0xBF}) // UTF-8 BOM

	l.file = f
	l.writer = bufio.NewWriter(f)
	return nil
}

// generateLogFilePath 生成带日期时间和序号的日志文件路径
func (l *Logger) generateLogFilePath() string {
	now := time.Now()
	dateStr := now.Format("2006-01-02_15-04-05")

	dir := filepath.Dir(l.filePath)
	baseName := filepath.Base(l.filePath)
	ext := filepath.Ext(baseName)
	nameWithoutExt := strings.TrimSuffix(baseName, ext)

	// 查找可用序号
	seq := 1
	for {
		fileName := fmt.Sprintf("%s_%s_%d%s", nameWithoutExt, dateStr, seq, ext)
		filePath := filepath.Join(dir, fileName)
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			return filePath
		}
		seq++
		if seq > 1000 {
			// 防止无限循环
			return filePath
		}
	}
}

// cleanupOldLogs 清理旧日志文件，只保留最近 N 个
func (l *Logger) cleanupOldLogs(maxFiles int) {
	dir := filepath.Dir(l.filePath)
	baseName := filepath.Base(l.filePath)
	ext := filepath.Ext(baseName)
	nameWithoutExt := strings.TrimSuffix(baseName, ext)

	// 读取目录中的所有日志文件
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}

	// 收集匹配的日志文件
	type logFile struct {
		path    string
		modTime time.Time
	}
	var logFiles []logFile

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		// 匹配格式：nameWithoutExt_YYYY-MM-DD_HH-MM-SS_N.ext
		if strings.HasPrefix(name, nameWithoutExt+"_") && strings.HasSuffix(name, ext) {
			info, err := entry.Info()
			if err != nil {
				continue
			}
			logFiles = append(logFiles, logFile{
				path:    filepath.Join(dir, name),
				modTime: info.ModTime(),
			})
		}
	}

	// 如果文件数不超过限制，不需要清理
	if len(logFiles) <= maxFiles {
		return
	}

	// 按修改时间排序（从新到旧）
	sort.Slice(logFiles, func(i, j int) bool {
		return logFiles[i].modTime.After(logFiles[j].modTime)
	})

	// 删除超出限制的最旧文件
	for i := maxFiles; i < len(logFiles); i++ {
		os.Remove(logFiles[i].path)
	}
}

// flushLoop 定时刷盘循环
func (l *Logger) flushLoop() {
	for {
		select {
		case <-l.flushTicker.C:
			l.flush()
		case <-l.closeChan:
			return
		}
	}
}

// flush 刷盘
func (l *Logger) flush() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.writer != nil {
		l.writer.Flush()
	}
}

// Close 关闭日志
func Close() error {
	if globalLogger == nil {
		return nil
	}

	globalLogger.mu.Lock()
	defer globalLogger.mu.Unlock()

	// 停止刷盘循环
	if globalLogger.flushTicker != nil {
		globalLogger.flushTicker.Stop()
		select {
		case <-globalLogger.closeChan:
		default:
			close(globalLogger.closeChan)
		}
	}

	// 最后一次刷盘
	if globalLogger.writer != nil {
		globalLogger.writer.Flush()
	}

	// 关闭文件
	if globalLogger.file != nil {
		return globalLogger.file.Close()
	}

	return nil
}

// String 日志级别转字符串
func (l Level) String() string {
	switch l {
	case DEBUG:
		return "DEBUG"
	case INFO:
		return "INFO"
	case WARN:
		return "WARN"
	case ERROR:
		return "ERROR"
	case FATAL:
		return "FATAL"
	default:
		return "UNKNOWN"
	}
}

// SetLevel 设置日志级别
func SetLevel(level Level) {
	globalLogger.mu.Lock()
	defer globalLogger.mu.Unlock()
	globalLogger.level = level
}

// SetPrefix 设置日志前缀
func SetPrefix(prefix string) {
	globalLogger.mu.Lock()
	defer globalLogger.mu.Unlock()
	globalLogger.prefix = prefix
}

// output 输出日志
func (l *Logger) output(level Level, format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if level < l.level {
		return
	}

	now := time.Now().Format("2006-01-02 15:04:05")
	prefix := l.prefix
	if prefix != "" {
		prefix = "[" + prefix + "] "
	}

	msg := fmt.Sprintf(format, args...)

	var line string
	if l.useJSON {
		// JSON 格式
		entry := LogEntry{
			Timestamp: now,
			Level:     level.String(),
			Prefix:    l.prefix,
			Message:   msg,
		}
		data, _ := json.Marshal(entry)
		line = string(data)
	} else {
		// 文本格式
		line = fmt.Sprintf("%s [%s] %s%s", now, level.String(), prefix, msg)
	}

	// 写入文件
	if l.useFile && l.writer != nil {
		l.writer.WriteString(line + "\n")
		// 如果是 FATAL 或 ERROR，立即刷盘
		if level >= ERROR {
			l.writer.Flush()
		}
	}

	// 输出到终端（如果启用了控制台输出）
	if l.useConsole {
		if level >= ERROR {
			fmt.Fprintln(os.Stderr, line)
		} else {
			fmt.Println(line)
		}
	}

	// FATAL 级别退出
	if level == FATAL {
		os.Exit(1)
	}
}

// Debug 输出 DEBUG 级别日志
func Debug(format string, args ...interface{}) {
	globalLogger.output(DEBUG, format, args...)
}

// Info 输出 INFO 级别日志
func Info(format string, args ...interface{}) {
	globalLogger.output(INFO, format, args...)
}

// Warn 输出 WARN 级别日志
func Warn(format string, args ...interface{}) {
	globalLogger.output(WARN, format, args...)
}

// Error 输出 ERROR 级别日志
func Error(format string, args ...interface{}) {
	globalLogger.output(ERROR, format, args...)
}

// Fatal 输出 FATAL 级别日志并退出
func Fatal(format string, args ...interface{}) {
	globalLogger.output(FATAL, format, args...)
}

// WithPrefix 创建带前缀的日志
func WithPrefix(prefix string) {
	globalLogger.mu.Lock()
	defer globalLogger.mu.Unlock()
	globalLogger.prefix = prefix
}

// ResetPrefix 重置日志前缀
func ResetPrefix() {
	globalLogger.mu.Lock()
	defer globalLogger.mu.Unlock()
	globalLogger.prefix = ""
}

// SetFile 动态设置日志文件
func SetFile(filePath string) error {
	globalLogger.mu.Lock()
	defer globalLogger.mu.Unlock()

	// 关闭旧文件
	if globalLogger.writer != nil {
		globalLogger.writer.Flush()
	}
	if globalLogger.file != nil {
		globalLogger.file.Close()
	}

	// 打开新文件
	globalLogger.filePath = filePath
	return globalLogger.openFile()
}

// DisableFile 禁用文件输出
func DisableFile() {
	globalLogger.mu.Lock()
	defer globalLogger.mu.Unlock()

	if globalLogger.writer != nil {
		globalLogger.writer.Flush()
	}
	if globalLogger.file != nil {
		globalLogger.file.Close()
		globalLogger.file = nil
		globalLogger.writer = nil
	}
	globalLogger.useFile = false
}

// EnableFile 启用文件输出
func EnableFile(filePath string) error {
	globalLogger.mu.Lock()
	defer globalLogger.mu.Unlock()

	globalLogger.filePath = filePath
	globalLogger.useFile = true
	return globalLogger.openFile()
}
