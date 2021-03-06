package rabbit

import (
	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/streadway/amqp"
)

// Publish sends a message to a rabbit connection
func Publish(channel *amqp.Channel, exchange, routingKey string, body []byte, reliable bool, headers map[string]interface{}) error {

	// Reliable publisher confirms require confirm.select support from the
	// connection.
	if reliable {
		log.Debug("enabling publishing confirms.")
		if err := channel.Confirm(false); err != nil {
			return fmt.Errorf("Channel could not be put into confirm mode: %s", err)
		}

		confirms := channel.NotifyPublish(make(chan amqp.Confirmation, 1))

		defer confirmOne(confirms)
	}

	log.Infof("publishing %dB body (%v)", len(body), string(body))
	if err := channel.Publish(
		exchange,   // publish to an exchange
		routingKey, // routing to 0 or more queues
		true,       // mandatory
		false,      // immediate
		amqp.Publishing{
			Headers:         headers,
			ContentType:     "text/plain",
			ContentEncoding: "",
			Body:            body,
			DeliveryMode:    amqp.Persistent, // 1=non-persistent, 2=persistent
			Priority:        0,               // 0-9

		},
	); err != nil {
		return fmt.Errorf("Exchange Publish: %s", err)
	}

	return nil
}

// One would typically keep a channel of publishings, a sequence number, and a
// set of unacknowledged sequence numbers and loop until the publishing channel
// is closed.
func confirmOne(confirms <-chan amqp.Confirmation) {
	log.Debugf("waiting for confirmation of one publishing")

	if confirmed := <-confirms; confirmed.Ack {
		log.Infof("confirmed delivery with delivery tag: %d", confirmed.DeliveryTag)
	} else {
		log.Errorf("failed delivery of delivery tag: %d", confirmed.DeliveryTag)
	}
}
