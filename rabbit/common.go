package rabbit

import (
	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/streadway/amqp"
)

// NewConnection builds a rabbitMq connection
func NewConnection(host, user, pass string, port int, vhost string) (*amqp.Connection, error) {

	amqpURI := fmt.Sprintf("amqp://%v:%v@%v:%v/%v", user, pass, host, port, vhost)
	log.Printf("dialing %q", amqpURI)
	connection, err := amqp.Dial(amqpURI)

	if err != nil {
		return nil, fmt.Errorf("Dial: %s", err)
	}

	return connection, nil
}

// DeclareExchange creates an Exchange
func DeclareExchange(channel *amqp.Channel, exchange, exchangeType string) error {
	log.Printf("got Channel, declaring %q Exchange (%q)", exchangeType, exchange)
	if err := channel.ExchangeDeclare(
		exchange,     // name
		exchangeType, // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // noWait
		nil,          // arguments
	); err != nil {
		return fmt.Errorf("Exchange Declare: %s", err)
	}
	return nil
}
