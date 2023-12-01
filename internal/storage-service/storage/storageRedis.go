package storage

import (
	"context"
	redis "github.com/redis/go-redis/v9"
)

type RedisStorageImpl struct {
	rdb *redis.ClusterClient
	ctx context.Context
}

func (s *RedisStorageImpl) init() {
	//s.rdb = redis.NewClusterClient(&redis.ClusterOptions{
	//	Addrs: []string{"172.28.1.10:7000", "172.28.1.11:7001", "172.28.1.12:7002", "172.28.1.13:7003", "172.28.1.14:7004", "172.28.1.15:7005"},
	//})

	var ctx = context.Background()

	clusterSlots := func(ctx context.Context) ([]redis.ClusterSlot, error) {
		slots := []redis.ClusterSlot{
			// First node with 1 master and 1 slave.
			{
				Start: 0,
				End:   8191,
				Nodes: []redis.ClusterNode{{
					Addr: "172.28.1.10:7000", // master
				}, {
					Addr: "172.28.1.13:7003", // 1st slave
				}},
			},
			// Second node with 1 master and 1 slave.
			{
				Start: 8192,
				End:   16383,
				Nodes: []redis.ClusterNode{{
					Addr: "172.28.1.11:7001", // master
				}, {
					Addr: "172.28.1.14:7004", // 2st slave
				}},
			},

			// Third node with 1 master and 1 slave.
			{
				Start: 16384,
				End:   24576,
				Nodes: []redis.ClusterNode{{
					Addr: "172.28.1.12:7003", // master
				}, {
					Addr: "172.28.1.15:7005", // 3st slave
				}},
			},
		}
		return slots, nil
	}

	s.rdb = redis.NewClusterClient(&redis.ClusterOptions{
		ClusterSlots:  clusterSlots,
		RouteRandomly: true,
	})

	s.rdb.Ping(ctx)

	// ReloadState reloads cluster state. It calls ClusterSlots func
	// to get cluster slots information.
	s.rdb.ReloadState(ctx)

}

func (s *RedisStorageImpl) Set(key string, value string) error {
	res := s.rdb.GetSet(s.ctx, key, value)
}

func (s *RedisStorageImpl) Get(key string) (string, error) {
	res := s.rdb.Get(s.ctx, key)
}
