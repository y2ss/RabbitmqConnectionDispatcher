package rabbitmq

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
)

type Producer struct {
	session Session
	channel *Channel
	tag     *string //多线程中用到
}

//non-thread-safe
func NewProducer(e Exchange, bo BindingOptions) *Producer {
	return &Producer{
		session: Session{
			Exchange: e,
			BindingOptions: bo,
		},
	}
}

//thread-safe
func NewSafeProducer(e Exchange, bo BindingOptions, unique string) *Producer {
	return &Producer{
		session: Session{
			Exchange: e,
			BindingOptions: bo,
		},
		tag: &unique,
	}
}

func (p *Producer) Publish(body []byte) (*Connection, error) {
	pool.mu.RLock()
	conn := pool.chooseIdleConnection(MQTypeProducer)
	pool.mu.RUnlock()
	if conn == nil {
		if pool.reachMaxConnection() {
			return nil, fmt.Errorf("Maximum number of connections reached")
		}
		//has no free connection  create new connection
		conns, err := pool.createNewConn(MQTypeProducer, true)
		if err != nil { return nil, err }
		//create new channel
		newCh, err := conns.createNewProducerChannel()
		if err != nil { return nil, err }

		err = p.bind(newCh)
		if err != nil {
			newCh.close(MQTypeProducer, nil)
			return nil, err
		}
		return conns, conns.doPublish(newCh, p.session, body)

	} else {
		//has free connection
		ch := conn.chooseIdleChannel()
		if ch != nil {
			if err := p.bind(ch); err != nil {
				if err1 := conn.occupied(ch, false, true); err1 != nil {
					//back to idle pool
					log.Printf("%s", err1.Error())
					ch.close(MQTypeProducer, nil)
				}
				return nil, err
			}
			return conn, conn.doPublish(ch, p.session, body)
		}

		//have idle connection but no idle channel, create new channel
		newCh, err := conn.createNewProducerChannel()
		if err != nil { return nil, err }

		err = p.bind(newCh)//bind exchange
		if err != nil {
			newCh.close(MQTypeProducer, nil)//do not put into idle channel pool
			return nil, err
		}
		return conn, conn.doPublish(newCh, p.session, body)
	}
}

//non-thread-safe
func (p *Producer) Shutdown(conn *Connection) error {
	//当空闲池的channel过多时把这个channel关闭
	//否则把当前channel放入空闲池
	//如果当前正在使用的channel为0 尝试断开这个connection
	if conn == nil { return fmt.Errorf("connection is nil") }
	return conn.shutdownIfNeeded(p.channelKey(), MQTypeProducer, nil)
}

func (p *Producer) bind(ch *Channel) error {
	p.channel = ch
	e := p.session.Exchange
	// declaring Exchange
	if err1 := p.channel.channel.ExchangeDeclare(
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

	p.channel.tag = p.channelKey()
	return nil
}

func (p *Producer)channelKey() string {
	if p.tag != nil && len(*p.tag) != 0 {
		return *p.tag + p.session.BindingOptions.RoutingKey + p.session.Exchange.Name
	}
	return p.session.BindingOptions.RoutingKey + p.session.Exchange.Name
}

//消息进入exchange但未进入queue时会被调用
func (p *Producer) NotifyReturn(notifier func(message amqp.Return)) {
	go func() {
		for res := range p.channel.channel.NotifyReturn(make(chan amqp.Return)) {
			notifier(res)
		}
	}()
}

//消息从生产者到达exchange时返回ack，消息未到达exchange返回nack
func (p *Producer) NotifyConfirm(ackFunc func(uint64), nackFunc func(uint64)) {
	go func() {
		ack, nack := p.channel.channel.NotifyConfirm(make(chan uint64), make(chan uint64))
		select {
		case an := <-ack:
			if ackFunc != nil { ackFunc(an) }
		case nan := <- nack:
			if nackFunc != nil { nackFunc(nan) }
		}
	}()
}