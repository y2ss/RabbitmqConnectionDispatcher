package memcache

import (
	"github.com/bradfitz/gomemcache/memcache"
	"RabbitmqConnectionDispatcher/config/bootstrap"
)

var MC *memcache.Client
func init() {
	config := bootstrap.App.AppConfig.Map("memcache")
	MC = memcache.New(config["host"])
}

func GetString(key string) (string, error) {
	item, err := MC.Get(key)
	if err != nil {
		return "", err
	}
	return string(item.Value), nil
}

func Get(key string) ([]byte, error) {
	item, err := MC.Get(key)
	if err != nil {
		return nil, err
	}
	return item.Value, nil
}

func SetString(key, value string) error {
	return MC.Set(&memcache.Item{
		Key: key,
		Value: []byte(value),
	})
}

func SetBytes(key string, value []byte, expired int32) error {
	return MC.Set(&memcache.Item{
		Key: key,
		Value: value,
		Expiration: expired,
	})
}

func CompareAndSwap(key, value string) error {
	return MC.CompareAndSwap(&memcache.Item{
		Key: key,
		Value: []byte(value),
	})
}

func Delete(key string) error {
	return MC.Delete(key)
}