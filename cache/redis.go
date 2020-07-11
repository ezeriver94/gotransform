package cache

import (
	"fmt"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/go-redis/cache/v8"
	"github.com/go-redis/redis/v8"
)

// NewCache creates a new redis cache
func NewCache(address, password string, port int) (*cache.Cache, error) {
	ring := redis.NewRing(&redis.RingOptions{
		Addrs: map[string]string{
			"server1": fmt.Sprintf("%v:%v", address, port),
		},
		Password: password,
	})

	mycache := cache.New(&cache.Options{
		Redis:      ring,
		LocalCache: fastcache.New(100 << 20), // 100 MB
	})
	return mycache, nil
}
