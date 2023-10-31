package config

import (
	"os"
	"strconv"
)

type LSMconfig struct {
	MtSize    uintptr
	SSTsegLen int64
	SSTDir    string
	JPath     string
}

func New() *LSMconfig {
	return &LSMconfig{
		MtSize:    uintptr(getEnvAsInt("MTSIZE", 300)),
		SSTsegLen: int64(getEnvAsInt("SSTABLESEGLEN", 100)),
		SSTDir:    getEnv("SSTABLEDIR", "ssTables"),
		JPath:     getEnv("JOURNALPATH", "WAL"),
	}
}

func getEnv(key string, defaultVal string) string {
	if v, exists := os.LookupEnv(key); exists {
		return v
	}

	return defaultVal
}

func getEnvAsInt(name string, defaultVal int) int {
	vStr := getEnv(name, "")
	if value, err := strconv.Atoi(vStr); err == nil {
		return value
	}

	return defaultVal
}
