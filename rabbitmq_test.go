package RabbitmqConnectionDispatcher

import (
	"log"
	"RabbitmqConnectionDispatcher/worker"
)

func Receiver() {
	exchange := rabbitmq.Exchange {
		Name:    "exchange",
		Type:    "direct",
		Durable: true,
	}
	queue := rabbitmq.Queue {
		Name:    "QUEUE",
		Durable: true,
	}
	binding := rabbitmq.BindingOptions {
		RoutingKey: "bind",
	}
	consumerOptions := rabbitmq.ConsumerOptions {
		Tag:     "BIND",
		AutoAck: false,
	}
	c := rabbitmq.NewConsumer(exchange, queue, binding, consumerOptions, "test")
	c.Qos(1)
	c.RegisterAutoReconnection(30, rabbitmq.FOREVER)
	log.Logger.Info("consumer worker started")
	err := c.Consume(consumerHandler)
	if err != nil {
		log.Logger.Error(err)
	}
}

var consumerHandler = func(delivery amqp.Delivery) {
	body := delivery.Body
	log.Logger.Info(body)
	worker.DispatchSerial(body, worker.WorkTypeONE, nil)
	delivery.Ack(false)
}