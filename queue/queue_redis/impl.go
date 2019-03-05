package queue_redis

import (
	"errors"
	"net"
	"time"

	"github.com/carltd/glib/queue"
	"github.com/carltd/glib/queue/message"
	"github.com/carltd/glib/queue/util"

	"github.com/garyburd/redigo/redis"
	"github.com/golang/protobuf/proto"
)

var (
	ErrSubFail = errors.New("queue redis: subscribe fail")
)

type redisSubscriber struct {
	subject string
	pbConn  redis.PubSubConn
}

func (s *redisSubscriber) NextMessage(timeout time.Duration) (*message.Message, error) {
	var start = time.Now()
	for cost := time.Duration(0); cost < timeout; cost = time.Now().Sub(start) {
		switch n := s.pbConn.ReceiveWithTimeout(timeout - cost).(type) {
		case redis.Message:
			ret := &message.Message{}
			err := proto.Unmarshal(n.Data, ret)
			return ret, err
		case redis.Subscription:
			if n.Count == 0 {
				return nil, ErrSubFail
			}
		case error:
			if e, ok := n.(net.Error); ok && e.Timeout() {
				return nil, queue.ErrTimeout
			}
			return nil, n
		}
	}
	return nil, queue.ErrTimeout
}

func (s *redisSubscriber) Close() error {
	return s.pbConn.Close()
}

type redisQueueConn struct {
	cs *redis.Pool
}

func (d *redisQueueConn) peekAvailableConn() (c redis.Conn, err error) {

	limit := 3
	for {
		c = d.cs.Get()
		if err = c.Err(); err != nil {
			_ = c.Close()
			limit--
			if limit == 0 {
				return nil, err
			}
			continue
		}
		return
	}
}

func (d *redisQueueConn) Ping() error {
	c, err := d.peekAvailableConn()
	if err != nil {
		return err
	}
	_, err = c.Do("PING")
	_ = c.Close()
	return err
}

func (d *redisQueueConn) Publish(subject string, msg *message.Message) error {
	if msg.MessageId == "" {
		msg.MessageId = util.GenMsgID()
	}
	c, err := d.peekAvailableConn()
	if err != nil {
		return err
	}

	buf, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	_ = c.Send("PUBLISH", subject, buf)
	_ = c.Flush()
	err = c.Err()
	_ = c.Close()
	return err
}

func (d *redisQueueConn) Subscribe(subject, group string) (queue.Subscriber, error) {
	c, err := d.peekAvailableConn()
	if err != nil {
		return nil, err
	}

	psC := redis.PubSubConn{Conn: c}
	if err = psC.Subscribe(subject); err != nil {
		return nil, err
	}
	return &redisSubscriber{pbConn: psC, subject: subject}, err
}

func (d *redisQueueConn) Enqueue(subject string, msg *message.Message) error {
	msg.MessageId = util.GenMsgID()
	c, err := d.peekAvailableConn()
	if err != nil {
		return err
	}

	buf, err := proto.Marshal(msg)
	if err != nil {
		_ = c.Close()
		return err
	}
	_ = c.Send("LPUSH", subject, buf)
	_ = c.Flush()
	err = c.Err()
	_ = c.Close()
	return err
}

func (d *redisQueueConn) Dequeue(subject, group string, timeout time.Duration, dst proto.Message) (*message.Meta, error) {
	c, err := d.peekAvailableConn()
	if err != nil {
		return nil, err
	}
	buf, err := redis.ByteSlices(c.Do("BRPOP", subject, timeout.Seconds()))
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	_ = c.Close()

	meta := &message.Meta{}
	ret := &message.Message{}
	if err = proto.Unmarshal(buf[1], ret); err != nil {
		return nil, err
	}
	meta.FormMessage(ret)
	meta.Src = string(buf[1])

	err = proto.Unmarshal(ret.Body, dst)
	return meta, err
}

func (d *redisQueueConn) Close() error {
	return d.cs.Close()
}
