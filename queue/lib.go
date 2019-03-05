package queue

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/carltd/glib/queue/message"
)

var ErrTimeout = errors.New("consumer get message timeout")

type Publisher interface {
	// Unicast mode
	Enqueue(subject string, msg *message.Message) error
	// Broadcast mode
	Publish(subject string, msg *message.Message) error
	io.Closer
}

type Subscriber interface {
	NextMessage(timeout time.Duration) (*message.Message, error)
	io.Closer
}

type Consumer interface {
	// Unicast mode
	Dequeue(subject, group string, timeout time.Duration, msg proto.Message) (*message.Meta, error)
	// Broadcast mode
	Subscribe(subject, group string) (Subscriber, error)
	io.Closer
}

type driver interface {
	OpenPublisher(addr string) (Publisher, error)
	OpenConsumer(addr string) (Consumer, error)
}

var (
	drivers   = make(map[string]driver)
	driversMu sync.RWMutex
)

// Register makes a queue driver available by the provided name.
// If Register is called twice with the same name or if driver is nil,
// it panics.
func Register(name string, d driver) {
	driversMu.Lock()
	defer driversMu.Unlock()
	if d == nil {
		panic("queue: Register driver is nil")
	}

	if _, dup := drivers[name]; dup {
		panic("queue: Register called twice for driver " + name)
	}
	drivers[name] = d
}

// Drivers returns a sorted list of the names of the registered drivers.
func Drivers() []string {
	driversMu.RLock()
	defer driversMu.RUnlock()
	var list []string
	for name := range drivers {
		list = append(list, name)
	}
	sort.Strings(list)
	return list
}

func NewPublisher(driverName, queueAddrs string) (Publisher, error) {
	driversMu.RLock()
	d, ok := drivers[driverName]
	driversMu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("queue: unknown driver %q (forgotten import?)", driverName)
	}

	return d.OpenPublisher(queueAddrs)
}

func NewConsumer(driverName, queueAddrs string) (Consumer, error) {
	driversMu.RLock()
	d, ok := drivers[driverName]
	driversMu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("queue: unknown driver %q (forgotten import?)", driverName)
	}

	return d.OpenConsumer(queueAddrs)
}
