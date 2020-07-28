package rabbitmq

import (
	"fmt"
	"github.com/streadway/amqp"
	"RabbitmqConnectionDispatcher/common/log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Consumer struct {
	session       Session
	channel       *Channel
	closeHandler  func(error *amqp.Error)
	HandlerClosed bool //default true
	QOS           int
	Tag           string
}

func NewConsumer(e Exchange, q Queue, bo BindingOptions, co ConsumerOptions, tag string) *Consumer {
	return &Consumer{
		session: Session{
			Exchange:        e,
			Queue:           q,
			ConsumerOptions: co,
			BindingOptions:  bo,
		},
		HandlerClosed: true,
		QOS: 3,
		Tag: tag,
	}
}

func (c *Consumer) Qos(prefetchCount int) {
	c.QOS = prefetchCount
}

func (c *Consumer) Consume(handler func(delivery amqp.Delivery)) error {
	pool.mu.Lock()
	conn := pool.chooseIdleConnection(MQTypeConsumer)
	if conn == nil {
		//has no free connection create new connection
		conns, err := pool.createNewConn(MQTypeConsumer, c.HandlerClosed)

		if err != nil {
			pool.mu.Unlock()
			return err
		}
		//create new channel
		newCh, err := conns.createNewConsumerChannel()
		if err != nil {
			pool.mu.Unlock()
			return err
		}

		err = c.bind(newCh)
		if err != nil {
			newCh.close(MQTypeConsumer, &c.session.ConsumerOptions)
			pool.mu.Unlock()
			return err
		}
		c.handlerClosedError(conns)
		if err := conns.occupied(newCh, true, true); err != nil {
			pool.mu.Unlock()
			return err
		}
		pool.mu.Unlock()
		return newCh.consumer(c.session.ConsumerOptions, c.session.BindingOptions, c.session.Queue, c.QOS, handler)

	} else {
		//has free connection
		newCh := conn.chooseIdleChannel()
		if newCh != nil {
			pool.mu.Unlock()
			err := c.bind(newCh)
			if err != nil {
				//back to idle pool
				if err1 := conn.occupied(newCh, false, true); err1 != nil {
					newCh.close(MQTypeConsumer, &c.session.ConsumerOptions)
				}
				return err
			}
			c.handlerClosedError(conn)
			return conn.doConsume(newCh, c.session, c.QOS, handler)
		}

		//have idle connection but no idle channel, create new channel
		newCh, err := conn.createNewConsumerChannel()
		if err != nil {
			pool.mu.Unlock()
			return err
		}

		err = c.bind(newCh)//bind exchange
		if err != nil {
			pool.mu.Unlock()
			newCh.close(MQTypeConsumer, &c.session.ConsumerOptions) //do not put into idle channel pool
			return err
		}
		c.handlerClosedError(conn)
		if err := conn.occupied(newCh, true, true); err != nil {
			pool.mu.Unlock()
			return err
		}
		pool.mu.Unlock()
		return newCh.consumer(c.session.ConsumerOptions, c.session.BindingOptions, c.session.Queue, c.QOS, handler)
	}
}

func (c *Consumer) handlerClosedError(conn *Connection) {
	if c.HandlerClosed == true && c.closeHandler != nil {
		conn.closeHandlers = append(conn.closeHandlers, c.closeHandler)
	}
}

//可能会将connection和channel放入连接池中
func (c *Consumer) Shutdown() error {
	err := shutdownChannel(c.channel.channel, c.session.ConsumerOptions.Tag)
	if err != nil { return err }

	defer log.Logger.Info("Consumer shutdown OK")
	log.Logger.Info("Waiting for Consumer handler to exit")

	err = lock(&pool.lock)
	if err != nil { return err }
	defer func(){ pool.lock = 0 }()

	if conn := c.usingConnection(); conn != nil {
		return conn.shutdownIfNeeded(c.channelKey(), MQTypeConsumer, &c.session.ConsumerOptions)
	} else {
		return fmt.Errorf("consumer shutdown error")
	}
}

//完全断开conn和channel
func (c *Consumer) Close() error {
	err := shutdownChannel(c.channel.channel, c.session.ConsumerOptions.Tag)
	if err != nil { log.Logger.Error(err) }
	defer log.Logger.Info("Consumer shutdown OK")
	log.Logger.Info("Waiting for Consumer handler to exit")

	err1 := lock(&pool.lock)
	if err1 != nil { return err1 }
	defer func(){ pool.lock = 0 }()

	if conn := c.usingConnection(); conn != nil {
		return conn.shutdownConn(c.channelKey())
	} else {
		return fmt.Errorf("consumer shutdown error")
	}
}

func (c *Consumer) usingConnection() *Connection {
	if c == nil || c.channel == nil { return nil }
	return pool.connections[c.channel.connTag]
}

func (c *Consumer) bind(ch *Channel) error {
	c.channel = ch
	e := c.session.Exchange
	q := c.session.Queue
	bo := c.session.BindingOptions

	var err error
	// declaring Exchange
	if err1 := c.channel.channel.ExchangeDeclare(
		e.Name,       // name of the exchange
		e.Type,       // type
		e.Durable,    // durable
		e.AutoDelete, // delete when complete
		e.Internal,   // internal
		e.NoWait,     // noWait
		e.Args,       // arguments
	); err1 != nil {
		return err1
	}

	// declaring Queue
	_, err1 := c.channel.channel.QueueDeclare(
		q.Name,       // name of the queue
		q.Durable,    // durable
		q.AutoDelete, // delete when usused
		q.Exclusive,  // exclusive
		q.NoWait,     // noWait
		q.Args,       // arguments
	)
	if err1 != nil {
		return err1
	}

	// binding Exchange to Queue
	if err = c.channel.channel.QueueBind(
		// bind to real queue
		q.Name,        // name of the queue
		bo.RoutingKey, // bindingKey
		e.Name,        // sourceExchange
		bo.NoWait,     // noWait
		bo.Args,       // arguments
	); err != nil {
		return err
	}

	c.channel.tag = c.channelKey()
	return nil
}

func (c *Consumer) channelKey() string {
	return c.session.Exchange.Name + c.session.BindingOptions.RoutingKey + c.session.Queue.Name
}

//需要在connection异常时自动重连调用这个方法 retryTimes: 0为永远尝试断连
func (c *Consumer) RegisterAutoReconnection(after time.Duration, retryTimes int) {
	c.RegisterClosedHandler(func(error *amqp.Error) {
		c.retryConnection(after, retryTimes)
	})
}

func (c *Consumer) retryConnection(after time.Duration, retryTimes int) {
	rt := 1
	copyHandler := c.channel.handler
	c.closeHandler = nil
	go func() {
		for {
			time.Sleep(after * time.Second)
			log.Logger.Info("retry connect ", c.session.BindingOptions.RoutingKey, " ", rt, " times")
			rt++
			c.HandlerClosed = false//不处理close通知
			if err := c.Consume(copyHandler); err != nil {
				log.Logger.Error(err)
			}
			if err := c.Close(); err != nil {
				log.Logger.Error(err)
			}
			if retryTimes != FOREVER && retryTimes < rt { break }
		}
		log.Logger.Info("give up retry connect ", c.session.BindingOptions.RoutingKey)
	}()
}

func (c *Consumer) ConnectionTag() int {
	if c.channel != nil {
		return c.channel.connTag
	}
	return -1
}

func (c *Consumer) RegisterSignalHandler() {
	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals)
		for {
			signal := <-signals
			switch signal {
			case syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGSTOP:
				err := c.Shutdown()
				if err != nil {
					panic(err)
				}
				os.Exit(1)
			}
		}
	}()
}

//执行顺序是先shutdown再调用这个closedhandler
func (c *Consumer) RegisterClosedHandler(handler func(error *amqp.Error)) {
	c.closeHandler = handler
}