package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ezeriver94/gotransform/env"
	log "github.com/sirupsen/logrus"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/go-redis/cache/v8"
	"github.com/go-redis/redis/v8"
)

var redisCache *cache.Cache = nil
var (
	cacheHost     string
	cachePassword string
	cachePort     int
	cacheEnabled  bool = false
)

func init() {
	cacheHost := env.GetString("REDIS_CACHE_HOST")
	cachePassword := env.GetString("REDIS_CACHE_PASSWORD")
	if len(cacheHost) == 0 {
		return
	}
	cacheEnabled = true
	err := env.Get("REDIS_CACHE_PORT", &cachePort, 6379, nil)
	if err != nil {
		log.Fatalf("error parsing REDIS_CACHE_PORT envVar: %v", err)
	}
	ring := redis.NewRing(&redis.RingOptions{
		Addrs: map[string]string{
			"server1": fmt.Sprintf("%v:%v", cacheHost, cachePort),
		},
		Password: cachePassword,
	})
	redisCache = cache.New(&cache.Options{
		Redis:      ring,
		LocalCache: fastcache.New(100 << 20), // 100 MB
	})
}

func valueToString(value interface{}) (string, error) {
	switch value.(type) {
	case string:
		return value.(string), nil
	default:
		result, err := json.Marshal(value)
		return string(result), err
	}
}

// Retrieve tries to get a value from redis cache (if setted); if found, it returns it, otherwise, the function get is executed and stored in cache, and returned
func Retrieve(key string, get func() (interface{}, error)) (string, error) {
	var stringResult string
	var err error
	if redisCache != nil {
		ctx := context.TODO()
		var result interface{}
		if err = redisCache.Get(ctx, key, &result); err != nil {
			log.Debugf("cache miss for key %v. fetching data", key)

			result, err = get()
			if err != nil {
				return "", err
			}
			stringResult, err = valueToString(result)
			if err != nil {
				return "", err
			}

			log.Debugf("saving key %v with value %v in cache", key, stringResult)
			if err := redisCache.Set(&cache.Item{
				Ctx:   ctx,
				Key:   key,
				Value: stringResult,
				TTL:   time.Hour,
			}); err != nil {
				log.Errorf("error saving on cache %v", err)
			} else {
				log.Debugf("cache key %v saved successfully", key)
			}

		}
	} else {
		result, err := get()
		if err != nil {
			return "", err
		}
		stringResult, err = valueToString(result)
		if err != nil {
			return "", err
		}
	}
	return stringResult, nil
}
