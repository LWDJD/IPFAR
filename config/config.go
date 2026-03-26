package config

// Config 主配置结构体
type Config struct {
	Language string `json:"language"`
}

// // ServerConfig 服务器配置
// type ServerConfig struct {
// 	Addr         string        `json:"addr"`
// 	ReadTimeout  time.Duration `json:"read_timeout"`
// 	WriteTimeout time.Duration `json:"write_timeout"`
// }

// // DatabaseConfig 数据库配置
// type DatabaseConfig struct {
// 	Driver string `json:"driver"`
// 	Dsn    string `json:"dsn"`
// }
