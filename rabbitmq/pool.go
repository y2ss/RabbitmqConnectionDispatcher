package rabbitmq

import (
	"fmt"
	"github.com/streadway/amqp"
	"RabbitmqConnectionDispatcher/common/log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var pool *Pool

type Pool struct {
	connections map[int]*Connection
	config      *Config
	lock        int32
	mu          *sync.RWMutex
}

func lock(l *int32) error {
	cc := time.Duration(0)
	for {
		cc++
		if c := atomic.CompareAndSwapInt32(l, 0, 2); c == true {
			break
		} else {
			runtime.Gosched()
			time.Sleep(200 * time.Millisecond)
		}
		if cc % 5000 == 0 {
			if cc >= 80000 {
				return fmt.Errorf("cas acquire lock failed")
			}
		}
	}
	return nil
}

func (p *Pool) createNewConn(mtype MQType, handlerClose bool) (*Connection, error) {
	conn, err := dial(p.config)
	if err != nil { return nil, err }

	if mtype == MQTypeProducer {
		err := lock(&p.lock)
		defer func() {
			p.lock = 0
		}()
		if err != nil {
			log.Logger.Error(err.Error())
			return nil, err
		}
	}

	c := &Connection {
		connection:    conn,
		idleChannels:  make([]*Channel, 0),
		usedChannels:  make(map[string]*Channel),
		mtype:         mtype,
		lock:          0,
		tag:           len(pool.connections) + 1,
		closeHandlers: make([]func(error *amqp.Error), 0),
	}
	if handlerClose {
		c.handleError()
	}
	if _, ok := p.connections[c.tag]; ok == true {
		log.Logger.Error("thread error - create new connection failed")
		return nil, err
	}
	p.connections[c.tag] = c
	if mtype == MQTypeProducer {
		p.lock = 0
	}
	return c, nil
}

//no-lock
//生产者和消费者加锁情况不一样 所以这里面不进行加锁
func (pool *Pool) chooseIdleConnection(mtype MQType) *Connection {
	for _, conns := range pool.connections {
		if conns.mtype == mtype {
			if conns.hasFreeConnection(mtype) {
				return conns
			}
		}
	}
	return nil
}

func (pool *Pool) reachMaxConnection() bool {
	//no-lock
	return len(pool.connections) >= MAX_CONNECTIONS
}

func (pool *Pool) shutdown(conn *Connection) {
	defer pool.mu.Unlock()
	pool.mu.Lock()
	conn.close()
	delete(pool.connections, conn.tag)
	log.Logger.Info("connection ", conn.tag, " shutdown")
}

func (pool *Pool) scheduleCG() {
	go func() {
		for {
			pool.mu.Lock()
			pc := 0
			for _, c := range pool.connections {
				if c.mtype == MQTypeProducer { pc++ }
			}
			for _, c := range pool.connections {
				if (c.mtype == MQTypeProducer && pc > 1 || c.connection.IsClosed()) {
					if c.connection.IsClosed() || (c.emptyConnection() && c.hasTooManyChannel(c.mtype)) {
						log.Logger.Info("release producer connection: ", c.tag, " type:", c.mtype)
						c.close()
					}
				}
			}
			pool.mu.Unlock()
			now := time.Now()
			next := now.Add(time.Second * 30)
			next = time.Date(next.Year(), next.Month(), next.Day(), next.Hour(), next.Minute(), next.Second(), 0, next.Location())
			t := time.NewTimer(next.Sub(now))
			<-t.C
		}
	}()
}

func InitPool(config *Config) {
	pool = &Pool{
		connections: make(map[int]*Connection),
		config: config,
		lock: 0,
		mu: new(sync.RWMutex),
	}
	cs, err := pool.createNewConn(MQTypeProducer, true)
	if err != nil {
		log.Logger.Panic(err)
	}
	ch, err := cs.createNewProducerChannel()
	if err != nil {
		log.Logger.Panic(err)
	}
	cs.idleChannels = append(cs.idleChannels, ch)
	pool.scheduleCG()
	MAX_PRODUCER_CHANNEL_PER_CONN = config.MaxProducerChannelPerConn
	MAX_CONSUMER_CHANNEL_PER_CONN = config.MaxConcusmerChannelPerConn
	MAX_CONNECTIONS = config.MaxConnectionsInPool
	log.Logger.Info("initialize rabbitmq connections pool")
}