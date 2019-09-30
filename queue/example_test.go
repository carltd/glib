package queue_test

import (
	"log"
	"strings"
	"time"

	"github.com/carltd/glib/v2/queue"
	"github.com/carltd/glib/v2/queue/message"
	_ "github.com/carltd/glib/v2/queue/queue_redis"
	"github.com/carltd/glib/v2/queue/util"
)

func ExampleNewPublisher() {
	pub, err := queue.NewPublisher("redis", "redis://:123456@localhost")
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close()

	err = pub.Publish("subject", &message.Message{})
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleNewConsumer() {
	c, err := queue.NewConsumer("redis", "redis://:123456@localhost")
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	sub, err := c.Subscribe("subject", "appName")
	if err != nil {
		log.Fatal(err)
	}

	msg, err := sub.NextMessage(time.Second)
	if err != nil {
		if !strings.Contains(err.Error(), "timeout") {
			log.Fatal(err)
		}
		// retry?
	}

	// t is the message's publish time
	t, _ := util.TimestampFromMessageID(msg.MessageId)
	_ = t

	_ = msg
	// msg.MessageId
	// msg.Priority
	// msg.Options    some options for the msg
	// msg.Body
	// ptypes.UnmarshalAny(msg.Body, &YourProtoBufferStructPoint)
}
