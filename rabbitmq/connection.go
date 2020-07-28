package rabbitmq

import (
	"fmt"
	"github.com/streadway/amqp"
	"RabbitmqConnectionDispatcher/common/log"
)

type Connection struct {
	connection    *amqp.Connection
	idleChannels  []*Channel
	usedChannels  map[string]*Channel
	mtype         MQType
	lock          int32
	tag           int
	closeHandlers []func(error *amqp.Error)
}

//non-thread-safe
func (conn *Connection) close() {
	if err := conn.connection.Close(); err != nil {
		log.Logger.Error("connection ", conn.tag, " close error: ", err.Error())
	} else {
		log.Logger.Info("connection ", conn.tag, " closed")
	}
	conn.usedChannels = nil
	conn.idleChannels = nil
	conn.lock = 0
	conn.connection = nil
	conn.closeHandlers = nil
}

func (conn *Connection) createNewConsumerChannel() (*Channel, error) {
	channel, err := conn.connection.Channel()
	if err != nil {
		return nil, err
	}
	ch := &Channel {
		channel: channel,
		connTag: conn.tag,
	}
	return ch, nil
}

func (conn *Connection) createNewProducerChannel() (*Channel, error) {
	channel, err := conn.connection.Channel()
	if err != nil {
		return nil, err
	}
	return &Channel{
		channel: channel,
		connTag: conn.tag,
	}, nil
}

func (conn *Connection) shutdownConn(key string) error {
	err := lock(&conn.lock)
	if err != nil { return err }

	defer func() { conn.lock = 0 }()

	if c := conn.usedChannels[key]; c != nil {
		c.close(conn.mtype, nil)
	}
	pool.mu.Lock()
	conn.close()
	delete(pool.connections, conn.tag)
	pool.mu.Unlock()
	return nil
}

func (conn *Connection) shutdownIfNeeded(key string, mqType MQType, co *ConsumerOptions) error {

	err := lock(&conn.lock)
	if err != nil { return err }

	defer func() { conn.lock = 0 }()
	if c := conn.usedChannels[key]; c != nil {
		delete(conn.usedChannels, key)
		if conn.hasTooManyChannel(mqType) {
			//release channel
			c.close(mqType, co)
			if conn.emptyConnection() {
				pool.mu.Lock()
				num := 0
				for _, d := range pool.connections {
					if d.mtype == mqType {
						num++
					}
				}
				if num > 1 {
					//只保留一个producer和一个consumer connection
					conn.close()
				}
				pool.mu.Unlock()
			}
		} else {
			//put into idle pool
			if err := conn.occupied(c, false, false); err != nil {
				log.Logger.Error(err.Error())
				c.close(mqType, co)
			} else {
				c.clean(mqType, co)
			}
		}
		return nil
	} else {
		return fmt.Errorf("the key does not exist")
	}
}

func (conn *Connection) IsClosed() bool {
	return conn.connection.IsClosed()
}

func (conn *Connection) handleError() {
	go func() {
		//正常关闭不会收到close通知
		for amqpErr := range conn.connection.NotifyClose(make(chan *amqp.Error)) {
			// if the computer sleeps then wakes longer than a heartbeat interval,
			// the connection will be closed by the client.
			// https://github.com/streadway/amqp/issues/82
			log.Logger.Info("connection ", conn.tag, " error: ", amqpErr.Error())
			if conn.closeHandlers != nil {
				for _, handler := range conn.closeHandlers {
					handler(amqpErr)
				}
			}
			pool.shutdown(conn)
		}
	}()
	go func() {
		for b := range conn.connection.NotifyBlocked(make(chan amqp.Blocking)) {
			if b.Active {
				log.Logger.Info("TCP blocked: %s"+b.Reason)
			} else {
				log.Logger.Info("TCP unblocked")
			}
		}
	}()
}

func (conn *Connection) doConsume(ch *Channel, s Session, qos int, handler func(delivery amqp.Delivery)) error {
	if err := conn.occupied(ch, true, true); err != nil {
		return err
	}
	return ch.consumer(s.ConsumerOptions, s.BindingOptions, s.Queue, qos, handler)
}

func (conn *Connection) doPublish(ch *Channel, s Session, body []byte) error {
	if err := conn.occupied(ch, true, true); err != nil {//put into used pool
		return err
	}
	return ch.publish(s.Exchange, s.BindingOptions, body)
}

func (conn *Connection) occupied(ch *Channel, t bool, lockc bool) error {
	if lockc {
		err := lock(&conn.lock)
		defer func() {
			conn.lock = 0
		}()
		if err != nil {
			log.Logger.Error("occupied - connection ", conn.tag, " ", err.Error())
			return err
		}
	}
	switch t {
	case true:
		if conn != nil && conn.usedChannels != nil && conn.usedChannels[ch.tag] == nil {
			conn.usedChannels[ch.tag] = ch
		} else {
			return fmt.Errorf("tag %s already exists, occupied failed", ch.tag)
		}
		break
	case false:
		conn.idleChannels = append(conn.idleChannels, ch)
		break
	}
	return nil
}

func (c *Connection) chooseIdleChannel() *Channel {
	//推荐一个线程使用一个channel
	err := lock(&c.lock)
	defer func() {
		c.lock = 0
	}()
	if err != nil {
		log.Logger.Error("connection ", c.tag, " ", err.Error())
		return nil
	}
	if len(c.idleChannels) > 0 {
		ch := c.idleChannels[0]
		c.idleChannels = c.idleChannels[1:]
		c.lock = 0
		return ch
	}
	return nil
}

//no-lock
func (c *Connection) hasTooManyChannel(mqType MQType) bool {
	if mqType == MQTypeConsumer {
		return len(c.idleChannels) + len(c.usedChannels) > MAX_CONSUMER_CHANNEL_PER_CONN
	} else {
		return len(c.idleChannels) + len(c.usedChannels) > MAX_PRODUCER_CHANNEL_PER_CONN
	}
}
//no-lock
func (c *Connection) emptyConnection() bool {
	return len(c.usedChannels) == 0
}
//no-lock
func (c *Connection) hasFreeConnection(mqType MQType) bool {
	if mqType == MQTypeConsumer {
		return len(c.usedChannels) < MAX_CONSUMER_CHANNEL_PER_CONN || len(c.idleChannels) > 0
	} else {
		return len(c.usedChannels) < MAX_PRODUCER_CHANNEL_PER_CONN || len(c.idleChannels) > 0
	}
}
