package config

import (
	"os"
	"strconv"
	"time"
)

type LSMconfig struct {
	MtSize      uintptr
	SSTsegLen   int64
	SSTDir      string
	JPath       string
	GCperiodSec time.Duration
	RedisTime   time.Duration
	Type        string
}

func New() (*LSMconfig, error) {
	gcPeriodSec, err := time.ParseDuration(os.Getenv("GCPERIODSEC"))
	redisTime, err := time.ParseDuration(os.Getenv("REDISTIME"))
	if err != nil {
		return nil, err
	}
	return &LSMconfig{
		MtSize:      uintptr(getEnvAsInt("MTSIZE", 300)),
		SSTsegLen:   int64(getEnvAsInt("SSTABLESEGLEN", 100)),
		SSTDir:      getEnv("SSTABLEDIR", "ssTables"),
		JPath:       getEnv("JOURNALPATH", "WAL"),
		GCperiodSec: gcPeriodSec,
		RedisTime:   redisTime,
		Type:        getEnv("TYPE", "avlTree"),
	}, nil
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
