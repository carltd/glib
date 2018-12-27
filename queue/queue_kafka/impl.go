package queue_kafka

import (
	"errors"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/carltd/glib/queue"
	"github.com/carltd/glib/queue/message"
	"github.com/golang/protobuf/proto"
)

var (
	ErrNotSupport = errors.New("queue kafka: Not Support")
)

type kafkaQueueConn struct {
	ReportErrors        bool
	ReportNotifications bool
	ServerVersion       sarama.KafkaVersion
	Addr                []string
	Producer            sarama.SyncProducer
}

func (c *kafkaQueueConn) Ping() error {
	return nil
}

// Unicast mode
func (c *kafkaQueueConn) Enqueue(subject string, msg *message.Message) error {
	return ErrNotSupport
}

// Unicast mode
func (c *kafkaQueueConn) Dequeue(subject, group string, timeout time.Duration, msg proto.Message) (*message.Meta, error) {
	return nil, ErrNotSupport
}

// Broadcast mode
func (c *kafkaQueueConn) Publish(subject string, msg *message.Message) error {
	var pm = new(sarama.ProducerMessage)
	pm.Topic = subject
	pm.Key = sarama.ByteEncoder(msg.MessageId)
	pm.Value = sarama.ByteEncoder(msg.Body)
	pm.Headers = make([]sarama.RecordHeader, 0)
	for k, v := range msg.Options {
		pm.Headers = append(pm.Headers, sarama.RecordHeader{
			Key: []byte(k), Value: []byte(v),
		})
	}
	part, offset, err := c.Producer.SendMessage(pm)
	_ = part
	_ = offset
	return err
}

// Broadcast mode
func (c *kafkaQueueConn) Subscribe(subject, group string) (queue.Subscriber, error) {

	cfg := cluster.NewConfig()
	cfg.Consumer.Return.Errors = c.ReportErrors
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Group.Return.Notifications = c.ReportNotifications
	cfg.Version = c.ServerVersion
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	consumer, err := cluster.NewConsumer(c.Addr, group, []string{subject}, cfg)
	if err != nil {
		return nil, err
	}

	return &kafkaSubscriber{consumer}, nil
}

func (c *kafkaQueueConn) Close() error {
	return c.Producer.Close()
}

type kafkaSubscriber struct {
	*cluster.Consumer
}

func (s *kafkaSubscriber) NextMessage(timeout time.Duration) (*message.Message, error) {
	var (
		err     error
		msg     *sarama.ConsumerMessage
		wrapMsg *message.Message
	)

	for {
		select {
		case err = <-s.Consumer.Errors():
			return nil, err
		case msg = <-s.Consumer.Messages():
			wrapMsg = new(message.Message)
			wrapMsg.MessageId = string(msg.Key)
			wrapMsg.Body = make([]byte, len(msg.Value))
			copy(wrapMsg.Body, msg.Value)

			wrapMsg.Options = make(map[string]string)
			// copy kafka headers to owner Options field
			for _, v := range msg.Headers {
				wrapMsg.Options[string(v.Key)] = string(v.Value)
			}
			wrapMsg.Options["kafka-offset"] = fmt.Sprint(msg.Offset)
			s.Consumer.MarkOffset(msg, "")
			return wrapMsg, nil
		case ntf, more := <-s.Consumer.Notifications():
			if more {
				// log.Printf("consumer rebalance: %#v", ntf)
				_ = ntf
				// TODO: do something?
			}
		}
	}

}
