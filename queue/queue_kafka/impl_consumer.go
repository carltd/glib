package queue_kafka

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/carltd/glib/queue"
	"github.com/carltd/glib/queue/message"
	"github.com/golang/protobuf/proto"
)

type kafkaConsumer struct {
	opts    *cluster.Config
	servers []string
}

// Unicast mode
func (c *kafkaConsumer) Dequeue(subject, group string, timeout time.Duration, msg proto.Message) (*message.Meta, error) {
	return nil, ErrNotSupport
}

// Broadcast mode
func (c *kafkaConsumer) Subscribe(topic, group string) (queue.Subscriber, error) {
	consumer, err := cluster.NewConsumer(c.servers, group, []string{topic}, c.opts)
	if err != nil {
		return nil, err
	}

	return &kafkaSubscriber{c: consumer, serverVersion: c.opts.Version}, nil
}

func (c *kafkaConsumer) Close() error { return nil }

type kafkaSubscriber struct {
	c             *cluster.Consumer
	serverVersion sarama.KafkaVersion
}

func (s *kafkaSubscriber) Close() error {
	return s.c.Close()
}

func (s *kafkaSubscriber) NextMessage(timeout time.Duration) (*message.Message, error) {
	var (
		err     error
		msg     *sarama.ConsumerMessage
		wrapMsg *message.Message
	)

	for {
		select {
		case <-time.After(timeout):
			return nil, queue.ErrTimeout
		case err = <-s.c.Errors():
			return nil, err
		case msg = <-s.c.Messages():
			wrapMsg = new(message.Message)
			wrapMsg.MessageId = string(msg.Key)
			wrapMsg.Body = make([]byte, len(msg.Value))
			copy(wrapMsg.Body, msg.Value)
			if len(msg.Headers) > 0 {
				if s.serverVersion.IsAtLeast(sarama.V0_11_0_0) {
					wrapMsg.Options = make(map[string]string)
					// copy kafka headers to owner Options field
					for _, v := range msg.Headers {
						wrapMsg.Options[string(v.Key)] = string(v.Value)
					}
					wrapMsg.Options["kafka-offset"] = fmt.Sprint(msg.Offset)
				}
			}
			s.c.MarkOffset(msg, "")
			return wrapMsg, nil
		case ntf, more := <-s.c.Notifications():
			if more {
				_ = ntf
				// TODO: do something?
			}
		}
	}

}
