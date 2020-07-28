package rabbitmq

import (
	"fmt"
	"github.com/streadway/amqp"
)

type MQType int

const (
	MQTypeProducer MQType = 1
	MQTypeConsumer MQType = 2
	FOREVER = 0
)

var (
	MAX_PRODUCER_CHANNEL_PER_CONN = 20
	MAX_CONSUMER_CHANNEL_PER_CONN = 20
	MAX_CONNECTIONS = 10
)

type Config struct {
	Host                 string
	Port                 int
	Username             string
	Password             string
	Vhost                string
	MaxConnectionsInPool int

	MaxProducerChannelPerConn  int
	MaxConcusmerChannelPerConn int
}

type Session struct {
	// Exchange declaration settings
	Exchange Exchange

	// Queue declaration settings
	Queue Queue

	// Binding options for current exchange to queue binding
	BindingOptions BindingOptions

	// Consumer options for a queue or exchange
	ConsumerOptions ConsumerOptions
}

type Exchange struct {
	// Exchange name
	Name string

	// Exchange type
	Type string

	// Durable exchanges will survive server restarts
	Durable bool

	// Will remain declared when there are no remaining bindings.
	AutoDelete bool

	// Exchanges declared as `internal` do not accept accept publishings.Internal
	// exchanges are useful for when you wish to implement inter-exchange topologies
	// that should not be exposed to users of the broker.
	Internal bool

	// When noWait is true, declare without waiting for a confirmation from the server.
	NoWait bool

	// amqp.Table of arguments that are specific to the server's implementation of
	// the exchange can be sent for exchange types that require extra parameters.
	Args amqp.Table
}

type Queue struct {
	// The queue name may be empty, in which the server will generate a unique name
	// which will be returned in the Name field of Queue struct.
	Name string

	// Check Exchange comments for durable
	Durable bool

	// Check Exchange comments for autodelete
	AutoDelete bool

	// Exclusive queues are only accessible by the connection that declares them and
	// will be deleted when the connection closes.  Channels on other connections
	// will receive an error when attempting declare, bind, consume, purge or delete a
	// queue with the same name.
	Exclusive bool

	// When noWait is true, the queue will assume to be declared on the server.  A
	// channel exception will arrive if the conditions are met for existing queues
	// or attempting to modify an existing queue from a different connection.
	NoWait bool

	// Check Exchange comments for Args
	Args amqp.Table
}

type ConsumerOptions struct {
	// The consumer is identified by a string that is unique and scoped for all
	// consumers on this channel.
	Tag string

	// When autoAck (also known as noAck) is true, the server will acknowledge
	// deliveries to this consumer prior to writing the delivery to the network.  When
	// autoAck is true, the consumer should not call Delivery.Ack
	AutoAck bool // autoAck

	// Check Queue struct documentation
	Exclusive bool // exclusive

	// When noLocal is true, the server will not deliver publishing sent from the same
	// connection to this consumer. (Do not use Publish and Consume from same channel)
	NoLocal bool // noLocal

	// Check Queue struct documentation
	NoWait bool // noWait

	// Check Exchange comments for Args
	Args amqp.Table // arguments
}

type BindingOptions struct {
	// Publishings messages to given Queue with matching -RoutingKey-
	// Every Queue has a default binding to Default Exchange with their Qeueu name
	// So you can send messages to a queue over default exchange
	RoutingKey string

	// Do not wait for a consumer
	NoWait bool

	// App specific data
	Args amqp.Table
}

func dial(config *Config) (*amqp.Connection, error) {
	conf := amqp.URI{
		Scheme:   "amqp",
		Host:     config.Host,
		Port:     config.Port,
		Username: config.Username,
		Password: config.Password,
		Vhost:    config.Vhost,
	}.String()
	conn, err := amqp.Dial(conf)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// shutdownChannel is a general closer function for channels
func shutdownChannel(channel *amqp.Channel, tag string) error {
	// This waits for a server acknowledgment which means the sockets will have
	// flushed all outbound publishings prior to returning.  It's important to
	// block on Close to not lose any publishings.
	if err := channel.Cancel(tag, true); err != nil {
		if amqpError, isAmqpError := err.(*amqp.Error); isAmqpError && amqpError.Code != 504 {
			return fmt.Errorf("AMQP connection close error: %s", err)
		}
	}
	if err := channel.Close(); err != nil {
		return err
	}
	return nil
}
