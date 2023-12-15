package impl

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisImpl is an implementation of X interface for storage
type RedisImpl struct {
	client *redis.ClusterClient

	// defaultTTL is a time-to-live for redis keys
	defaultTTL time.Duration
}

// NewRedis returns a new instance of RedisImpl.
func NewRedis(defaultTTL time.Duration) *RedisImpl {
	fmt.Println("Creating redis implementation")
	return &RedisImpl{
		defaultTTL: defaultTTL,
	}
}

// Connect establish connection to redis cluster and makes ping.
func (r *RedisImpl) Connect() error {
	r.client = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{
			"172.28.1.10:6379",
			"172.28.1.11:6379",
			"172.28.1.12:6379",
			"172.28.1.13:6379",
			"172.28.1.14:6379",
			"172.28.1.15:6379",
		},
		Password: "bitnami",
	})

	//if err := r.client.Ping(ctx).Err(); err != nil {
	//	return fmt.Errorf("ping redis cluster: %w", err)
	//}

	return nil
}

func (r *RedisImpl) Set(ctx context.Context, key, value string) error {
	return r.client.Set(ctx, key, value, r.defaultTTL).Err()
}

func (r *RedisImpl) Get(ctx context.Context, key string) (string, error) {
	return r.client.Get(ctx, key).Result()
}
