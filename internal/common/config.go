// Package common provides shared utilities for IONIS applications.
package common

import (
	"os"
	"path/filepath"
)

// Config holds common configuration for all applications.
type Config struct {
	ClickHouseHost     string
	ClickHousePort     int
	ClickHouseDatabase string
	ClickHouseUser     string
	ClickHousePassword string
	DataDir            string
	LogLevel           string
}

// DefaultConfig returns configuration with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		ClickHouseHost:     getEnv("CLICKHOUSE_HOST", "localhost"),
		ClickHousePort:     9000,
		ClickHouseDatabase: getEnv("CLICKHOUSE_DATABASE", "wspr"),
		ClickHouseUser:     getEnv("CLICKHOUSE_USER", "default"),
		ClickHousePassword: getEnv("CLICKHOUSE_PASSWORD", ""),
		DataDir:            getEnv("IONIS_DATA_DIR", "/var/lib/ionis"),
		LogLevel:           getEnv("LOG_LEVEL", "info"),
	}
}

// WSPRDataDir returns the WSPR data directory path.
func (c *Config) WSPRDataDir() string {
	return filepath.Join(c.DataDir, "wspr")
}

// SolarDataDir returns the solar data directory path.
func (c *Config) SolarDataDir() string {
	return filepath.Join(c.DataDir, "solar")
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
