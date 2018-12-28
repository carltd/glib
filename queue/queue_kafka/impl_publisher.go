package queue_kafka

import (
	"time"

	"github.com/Shopify/sarama"
	"github.com/carltd/glib/queue/message"
)

type kafkaProducer struct {
	serverVersion sarama.KafkaVersion
	p             sarama.SyncProducer
}

// Unicast mode
func (c *kafkaProducer) Enqueue(subject string, msg *message.Message) error {
	return ErrNotSupport
}

// Broadcast mode
func (c *kafkaProducer) Publish(topic string, msg *message.Message) error {
	var pm = new(sarama.ProducerMessage)
	pm.Topic = topic
	pm.Timestamp = time.Now()
	pm.Key = sarama.ByteEncoder(msg.MessageId)
	pm.Value = sarama.ByteEncoder(msg.Body)
	if len(msg.Options) > 0 {
		if c.serverVersion.IsAtLeast(sarama.V0_11_0_0) {
			pm.Headers = make([]sarama.RecordHeader, 0)
			for k, v := range msg.Options {
				pm.Headers = append(pm.Headers, sarama.RecordHeader{
					Key: []byte(k), Value: []byte(v),
				})
			}
		}
	}
	_, _, err := c.p.SendMessage(pm)
	return err
}

func (c *kafkaProducer) Close() error {
	return c.p.Close()
}
