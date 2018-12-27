package queue_kafka

import (
	"time"

	"github.com/Shopify/sarama"
	"github.com/carltd/glib/queue/message"
)

type kafkaProducer struct {
	Producer sarama.SyncProducer
}

// Unicast mode
func (c *kafkaProducer) Enqueue(subject string, msg *message.Message) error {
	return ErrNotSupport
}

// Broadcast mode
func (c *kafkaProducer) Publish(subject string, msg *message.Message) error {
	var pm = new(sarama.ProducerMessage)
	pm.Topic = subject
	pm.Timestamp = time.Now()
	pm.Key = sarama.ByteEncoder(msg.MessageId)
	pm.Value = sarama.ByteEncoder(msg.Body)
	if len(msg.Options) > 0 {
		pm.Headers = make([]sarama.RecordHeader, 0)
		for k, v := range msg.Options {
			pm.Headers = append(pm.Headers, sarama.RecordHeader{
				Key: []byte(k), Value: []byte(v),
			})
		}
	}
	part, offset, err := c.Producer.SendMessage(pm)
	_ = part
	_ = offset
	return err
}

func (c *kafkaProducer) Close() error {
	return c.Producer.Close()
}
