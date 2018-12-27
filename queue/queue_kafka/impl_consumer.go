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

	return &kafkaSubscriber{consumer}, nil
}

func (c *kafkaConsumer) Close() error {
	// TODO: 需要实现
	return nil
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
			if len(msg.Headers) > 0 {
				wrapMsg.Options = make(map[string]string)
				// copy kafka headers to owner Options field
				for _, v := range msg.Headers {
					wrapMsg.Options[string(v.Key)] = string(v.Value)
				}
				wrapMsg.Options["kafka-offset"] = fmt.Sprint(msg.Offset)
			}
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
