package RabbitmqConnectionDispatcher

import (
	"log"
	"net/http"
	"strconv"
	"RabbitmqConnectionDispatcher/config/bootstrap"
	"RabbitmqConnectionDispatcher/rabbitmq"
)

func main() {
	rmqConfig := bootstrap.App.AppConfig.Map("rabbitmq")
	port, _ := strconv.Atoi(rmqConfig["port"])
	poolConfig := bootstrap.App.AppConfig.Map("rabbitmq.pool")
	mc, _ := strconv.Atoi(poolConfig["max_conn"])
	mpcpc, _ := strconv.Atoi(poolConfig["max_producer_channel_pre_conn"])
	mccpc, _ := strconv.Atoi(poolConfig["max_consumer_channel_pre_conn"])

	config := &rabbitmq.Config{
		Host:     rmqConfig["host"],
		Port:     port,
		Username: rmqConfig["username"],
		Password: rmqConfig["password"],
		Vhost:    rmqConfig["vhost"],
		MaxConnectionsInPool: mc,
		MaxProducerChannelPerConn: mpcpc,
		MaxConcusmerChannelPerConn: mccpc,
	}
	rabbitmq.InitPool(config)
	go Receiver()

	port := bootstrap.App.AppConfig.String("port")
	go func(p string) {
		http.ListenAndServe(":"+p, trace.InitRouter())
	}(port)

	log.Logger.Info("begin service")
	select {}
}