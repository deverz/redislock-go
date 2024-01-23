package redislock_go

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"log"
	"time"
)

var ErrLockOvertime = errors.New("redislock:get lock overtime")
var ErrLockFailed = errors.New("redislock:get lock failed")
var ErrUnlockValEmpty = errors.New("redislock:unlock val is empty")

type RedisLock struct {
	rds       *redis.Client // redis客户端
	key       string        // 锁的key
	val       string        // 锁的value
	ttl       time.Duration // 锁的过期时间-只支持1s以上的时间，小于1s默认为1s
	waitTime  time.Duration // 获取锁等待时间
	isWatch   bool          // 是否开启watch自动续期
	watchTime time.Duration // 开启watch后，最大监控持续时间，小于等于ttl表示不监控，等同于isWatch==false
	watchDone chan struct{} // 开启watch后，用于通知续期协程退出
}

// GetInstance 获取redis锁实例 - ！！！注意，请使用同一个实例加锁和解锁
func GetInstance(rds *redis.Client, key string, ttl, waitTime time.Duration, isWatch bool, watchTime time.Duration) *RedisLock {
	if ttl < time.Second {
		ttl = time.Second
	}
	isWatch = isWatch && watchTime > ttl
	var watchDone chan struct{}
	if isWatch {
		watchDone = make(chan struct{})
	}
	return &RedisLock{
		rds:       rds,
		key:       key,
		val:       uuid.NewString(),
		ttl:       ttl,
		waitTime:  waitTime,
		isWatch:   isWatch,
		watchTime: watchTime,
		watchDone: watchDone,
	}
}

// Lock 加锁
func (r *RedisLock) Lock(ctx context.Context) (err error) {
	// 如果开启watch，则加锁成功后自动开启锁续期
	defer func() {
		if err == nil && r.isWatch {
			go r.watch()
		}
	}()
	// 获取等待获取锁的超时时间
	waitMilli := time.Now().Add(r.waitTime).UnixMilli()
	for {
		var ok bool
		ok, err = r.rds.SetNX(ctx, r.key, r.val, r.ttl).Result()
		// 报错，返回
		if err != nil {
			return err
		}
		// 直接加锁成功，返回
		if ok {
			return nil
		}
		if r.waitTime == 0 {
			return ErrLockFailed
		}
		// 如果等待时间小于当前时间，则返回获取锁超时
		if waitMilli < time.Now().UnixMilli() {
			return ErrLockOvertime
		}
		// 休眠50毫秒，再次获取redis锁
		time.Sleep(time.Millisecond * 50)
	}
}

// watch 锁自动续期
func (r *RedisLock) watch() {
	if !r.isWatch {
		return
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), r.watchTime)
	defer cancelFunc()
	lua := `if redis.call("get", KEYS[1]) == ARGV[1] then
		redis.call("expire", KEYS[1], ARGV[2])
		return "1"
    else
        return "0"
    end`

	for {
		select {
		case <-ctx.Done():
			log.Printf("redislock watch timeout. key:%s, value:%s", r.key, r.val)
			return
		case <-r.watchDone:
			log.Printf("redislock watch done. key:%s, value:%s", r.key, r.val)
			return
		default:
			// 休眠过期时间的一半，跳出后，直接进行续期操作
			time.Sleep(r.ttl / 2)
		}

		// 进行续期，重新设置为ttl，用lua脚本实现，判断value值相同则进行续期，不同则结束watch
		resp, err := r.rds.Eval(ctx, lua, []string{r.key}, r.val, r.ttlSec()).Result()
		if err != nil {
			log.Printf("redislock watch set ttl failed. key:%s, value:%s, err:%v", r.key, r.val, err)
			return
		}
		// 返回值是0，则表示value值已经更换，无需再进行watch
		if fmt.Sprintf("%s", resp) == "0" {
			log.Printf("redislock watch value has change. key:%s, value:%s", r.key, r.val)
			return
		}
		// 表示续期成功，继续watch
		log.Printf("redislock watch set ttl success. key:%s, value:%s", r.key, r.val)
	}
}

// Unlock 解锁
func (r *RedisLock) Unlock(ctx context.Context) error {
	if r.isWatch {
		r.watchDone <- struct{}{}
		close(r.watchDone)
	}
	if r.val == "" {
		return ErrUnlockValEmpty
	}
	lua := `if redis.call("get", KEYS[1]) == ARGV[1] then
		return redis.call("del", KEYS[1])
    else
        return "0"
    end`
	_, err := r.rds.Eval(ctx, lua, []string{r.key}, r.val).Result()
	return err
}

// ttlSec 获取过期时间，单位秒
func (r *RedisLock) ttlSec() int64 {
	return int64(r.ttl / time.Second)
}
