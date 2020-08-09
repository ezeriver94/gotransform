package rabbit

import (
	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/streadway/amqp"
)

// Consumer represent a rabbit mq consumer wich subscribes to a channel and handles messages
type Consumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	queue   string
	tag     string
	Done    chan error
}

// Handle is a function that keeps listening for messages in a rabbit mq excahnge and performs and action
type Handle func(deliveries <-chan amqp.Delivery)

// NewConsumer creates a rabbitmq consumer
func NewConsumer(connection *amqp.Connection, channel *amqp.Channel, exchange, exchangeType, queueName, key, ctag string) (*Consumer, error) {
	c := &Consumer{
		conn:    connection,
		channel: channel,
		tag:     ctag,
		queue:   queueName,
		Done:    make(chan error),
	}

	var err error

	log.Infof("got Channel, declaring Exchange (%q)", exchange)
	if err = c.channel.ExchangeDeclare(
		exchange,     // name of the exchange
		exchangeType, // type
		true,         // durable
		false,        // delete when complete
		false,        // internal
		false,        // noWait
		nil,          // arguments
	); err != nil {
		return nil, fmt.Errorf("Exchange Declare: %s", err)
	}

	log.Infof("declared Exchange, declaring Queue %q", queueName)
	queue, err := c.channel.QueueDeclare(
		queueName, // name of the queue
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // noWait
		nil,       // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Declare: %s", err)
	}

	log.Infof("declared Queue (%q %d messages, %d consumers), binding to Exchange (key %q)",
		queue.Name, queue.Messages, queue.Consumers, key)

	if err = c.channel.QueueBind(
		queue.Name, // name of the queue
		key,        // bindingKey
		exchange,   // sourceExchange
		false,      // noWait
		nil,        // arguments
	); err != nil {
		return nil, fmt.Errorf("Queue Bind: %s", err)
	}

	return c, nil
}

// Consume starts listening for messages in a consumer by the execution of the handle function
func (c *Consumer) Consume(handle Handle) error {
	log.Infof("Queue bound to Exchange, starting Consume (consumer tag %q)", c.tag)
	deliveries, err := c.channel.Consume(
		c.queue, // name
		c.tag,   // consumerTag,
		false,   // noAck
		false,   // exclusive
		false,   // noLocal
		false,   // noWait
		nil,     // arguments
	)
	if err != nil {
		return fmt.Errorf("Queue Consume: %s", err)
	}
	rabbitCloseError := make(chan *amqp.Error)
	c.conn.NotifyClose(rabbitCloseError)
	handled := false
	for {
		select {
		case rabbitErr := <-rabbitCloseError:
			return rabbitErr
		default:
			if handled {
				return nil
			}
			handled = true
			handle(deliveries)
		}
	}
}

// Shutdown closes a consumer connection
func (c *Consumer) Shutdown() error {
	// will close() the deliveries channel
	if err := c.channel.Cancel(c.tag, true); err != nil {
		return fmt.Errorf("Consumer cancel failed: %s", err)
	}

	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}

	defer log.Infof("AMQP shutdown OK")

	// wait for handle() to exit
	return <-c.Done
}
