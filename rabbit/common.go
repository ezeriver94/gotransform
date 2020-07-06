package rabbit

import (
	"fmt"
	"log"

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

// func (conn *amqp.Connection) NewChannel(*amqp.Channel, error) {
// 	log.Printf("got Connection, getting Channel")
// 	channel, err := conn.Channel()
// 	if err != nil {
// 		return nil, fmt.Errorf("Channel: %s", err)
// 	}

// 	return channel, nil
// }
