package config

import (
	"os"
)

// Config 应用配置
type Config struct {
	HBase  HBaseConfig
	Server ServerConfig
}

// HBaseConfig HBase数据库配置
type HBaseConfig struct {
	Host       string
	ZkQuorum   string
	ZkPort     string
	MasterPort string
	ThriftPort string
}

// ServerConfig 服务器配置
type ServerConfig struct {
	Port string
}

// GetConfig 获取配置
func GetConfig() *Config {
	return &Config{
		HBase: HBaseConfig{
			Host:       getEnv("HBASE_HOST", "192.168.2.24"),
			ZkQuorum:   getEnv("HBASE_ZKQUORUM", "192.168.2.24"),
			ZkPort:     getEnv("HBASE_ZKPORT", "2181"),
			MasterPort: getEnv("HBASE_MASTERPORT", "16000"),
			ThriftPort: getEnv("HBASE_THRIFTPORT", "9090"),
		},
		Server: ServerConfig{
			Port: getEnv("SERVER_PORT", "5000"),
		},
	}
}

// getEnv 获取环境变量，若环境变量不存在则返回默认值
func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}
