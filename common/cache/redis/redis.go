package redis

import (
	"github.com/garyburd/redigo/redis"
	"RabbitmqConnectionDispatcher/common/log"
	"RabbitmqConnectionDispatcher/config/bootstrap"
	"strconv"
	"time"
)

var Pool *redis.Pool

const NIL = -1

func init() {
	config := bootstrap.App.AppConfig.Map("redis")
	idleTimeout, _ := strconv.Atoi(config["idleTimeout"])
	maxIdle, _ := strconv.Atoi(config["maxIdle"])
	maxActive, _ := strconv.Atoi(config["maxActive"])
	pool := &redis.Pool{
		MaxIdle: maxIdle,
		MaxActive: maxActive,
		IdleTimeout: time.Duration(idleTimeout),
		Wait: false,
		Dial: func() (redis.Conn, error) {
			if c, err := redis.Dial("tcp", config["host"], redis.DialPassword(config["password"])); err == nil {
				//log.Logger.Info("connected to redis")
				return c, nil
			} else {
				log.Logger.Error("lost redis connect")
				return nil, err
			}
		},
	}
	c := pool.Get()
	defer c.Close()
	Pool = pool
}

func Ping() error {
	c := Pool.Get()
	defer c.Close()
	_, err := c.Do("ping")
	return err
}

func GetInt(key string) (int, error) {
	c := Pool.Get()
	defer c.Close()
	res, err := redis.Int(c.Do("get", key))
	if err != nil {
		return NIL, err
	}
	return res, nil
}

func GetInt64(key string) (int64, error) {
	c := Pool.Get()
	defer c.Close()
	res, err := redis.Int64(c.Do("get", key))
	if err != nil {
		return NIL, err
	}
	return res, nil
}

func GetString(key string) (string, error) {
	c := Pool.Get()
	defer c.Close()
	res, err := redis.String(c.Do("get", key))
	if err != nil {
		return "", err
	}
	return res, nil
}

func Remove(key string) {
	c := Pool.Get()
	defer c.Close()
	c.Do("del", key)
}

func GetStringMap(key string) (map[string]string, error) {
	c := Pool.Get()
	defer c.Close()
	res, err := redis.StringMap(c.Do("hgetall", key))
	if err != nil {
		return nil, err
	}
	return res, nil
}

func Set(key string, value interface{}, seconds int) error {
	c := Pool.Get()
	defer c.Close()
	_, err := c.Do("set", key, value, "EX", seconds)
	if err != nil {
		return err
	}
	return nil
}

func Expire(key string, second int) error {
	c := Pool.Get()
	defer c.Close()
	_, err := c.Do("expire", key, second)
	if err != nil {
		return err
	}
	return nil
}

func SetIntAndExpire(key string, value int, second int) error {
	c := Pool.Get()
	defer c.Close()
	_, err := c.Do("set", key, value, "EX", second)
	if err != nil {
		return err
	}
	return nil
}

func SetMapAndExpire(key string, m map[string]string, second int) error {
	//TODO pipelining
	c := Pool.Get()
	defer c.Close()
	newArgs := make([]interface{}, 0)
	newArgs = append(newArgs, key)
	for k, v := range m {
		newArgs = append(newArgs, k)
		newArgs = append(newArgs, v)
	}
	_, err := c.Do("hmset", newArgs...)
	if err != nil {
		return err
	}
	c.Do("expire", key, second)
	return nil
}


