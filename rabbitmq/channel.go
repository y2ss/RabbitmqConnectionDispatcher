package rabbitmq

import (
	"github.com/streadway/amqp"
	"RabbitmqConnectionDispatcher/common/log"
)

type Channel struct {
	channel       *amqp.Channel

	tag           string
	// All deliveries from server will send to this channel
	deliveries <-chan amqp.Delivery
	// This handler will be called when a
	handler func(amqp.Delivery)

	connTag int
}

func (c *Channel) clean(mqType MQType, co *ConsumerOptions) {
	if (mqType == MQTypeConsumer) {
		c.handler = nil
		if co != nil {
			// This waits for a server acknowledgment which means the sockets will have
			// flushed all outbound publishings prior to returning.  It's important to
			// block on Close to not lose any publishings.
			if err := c.channel.Cancel(co.Tag, true); err != nil {
				log.Logger.Error(err.Error())
			}
		}
	}
	c.tag = ""
}

func (c *Channel) close(mqType MQType, co *ConsumerOptions) {
	c.clean(mqType, co)
	if err := c.channel.Close(); err != nil {
		log.Logger.Error(err.Error())
	}
}

func (ch *Channel) publish(e Exchange, b BindingOptions, body []byte) error {
	//mandatory: 当mandatory标志位设置为true时，如果exchange根据自身类型和消息routeKey无法找到一个符合条件的queue，那么会将消息返回给生产者
	//当mandatory设置为false时，出现上述情形broker会直接将消息扔掉
	//immediate: 当immediate标志位设置为true时，如果exchange在将消息路由到queue(s)时发现对于的queue上么有消费者，那么这条消息不会放入队列中。
	//当与消息routeKey关联的所有queue（一个或者多个）都没有消费者时，该消息会通过basic.return方法返还给生产者。
	err := ch.channel.Publish(e.Name, b.RoutingKey, false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body: body,
	})
	return err
}

func (ch *Channel) consumer(co ConsumerOptions, b BindingOptions, q Queue, qos int, handler func(delivery amqp.Delivery)) error {
	//prefetchCount: 一旦有N个消息还没有ack，则该consumer将block掉，直到有消息ack
	//global: 全局channel还是该channel
	//只有在autoAck为false时有效
	//同时每个consumer都会预取n个消息 注意多进程多消费者消费同一个数据的问题
	err := ch.channel.Qos(qos, 0, false)
	if err != nil {
		return err
	}
	// Exchange bound to Queue, starting Consume
	deliveries, err := ch.channel.Consume(
		// consume from real queue
		q.Name,       // name
		co.Tag,       // consumerTag,
		co.AutoAck,   // autoAck
		co.Exclusive, // exclusive
		co.NoLocal,   // noLocal
		co.NoWait,    // noWait
		co.Args,      // arguments
	)
	if err != nil {
		return err
	}
	ch.deliveries = deliveries
	ch.handler = handler

	log.Logger.Info("handle:", b.RoutingKey, " deliveries channel starting, connection tag ", ch.connTag)
	// handle all consumer errors, if required re-connect
	// there are problems with reconnection logic for now
	for delivery := range ch.deliveries {
		handler(delivery)
	}
	log.Logger.Info("handle:", b.RoutingKey, " deliveries channel closed, connection tag ", ch.connTag)
	return nil
}

