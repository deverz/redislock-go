package redislock_go

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"log"
	"testing"
	"time"
)

func TestRedisLock(t *testing.T) {
	ctx := context.Background()
	rl := GetInstance(
		makeRedisClient(),
		"test-lock-lock",
		10*time.Second,
		0,
		true,
		5*time.Minute)
	err := rl.Lock(ctx)
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(1 * time.Minute)
	err = rl.Unlock(ctx)
	if err != nil {
		log.Fatal(err)
	}
}

type Config struct {
	Host           string        `ini:"host"`
	Port           int           `ini:"port,default(6379)"`
	User           string        `ini:"user"`
	Password       string        `ini:"password"`
	Database       int           `ini:"database,default(0)"`
	ConnectTimeout time.Duration `ini:"connect_timeout,default(5s)"`
	ReadTimeout    time.Duration `ini:"read_timeout,default(3s)"`
	WriteTimeout   time.Duration `ini:"write_timeout,default(3s)"`
}

// [redis]
//host = r-2zeyte1lip7jceqmerpd.redis.rds.aliyuncs.com
//port = 6379
//user =
//password = yCHW3BWBYhWTe6GbMp2Z
//database = 4
//# 连接建立超时时间 时间单位： "ns", "us" (or "µs"), "ms", "s", "m", "h"
//connect_timeout = 5s
//read_timeout = 3s
//write_timeout = 3s

func makeRedisClient() *redis.Client {
	conf := Config{
		Host:           "r-2zeyte1lip7jceqmerpd.redis.rds.aliyuncs.com",
		Port:           6379,
		User:           "",
		Password:       "yCHW3BWBYhWTe6GbMp2Z",
		Database:       4,
		ConnectTimeout: 5 * time.Second,
		ReadTimeout:    3 * time.Second,
		WriteTimeout:   3 * time.Second,
	}
	client := redis.NewClient(&redis.Options{
		Addr:         fmt.Sprintf("%s:%d", conf.Host, conf.Port),
		Username:     conf.User,
		Password:     conf.Password,
		DB:           conf.Database,
		DialTimeout:  conf.ConnectTimeout,
		ReadTimeout:  conf.ReadTimeout,
		WriteTimeout: conf.WriteTimeout,
	})
	return client
}
