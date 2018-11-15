package glib

import (
	"fmt"
	"sync"

	"github.com/carltd/glib/queue"
)

type brokerConfig struct {
	Enable bool   `json:"enable"`
	Alias  string `json:"alias"`
	Type   string `json:"type"`
	Dsn    string `json:"dsn"`
	Driver string `json:"driver"`
}

var (
	publishers sync.Map
	consumers  sync.Map
)

const (
	BrokerTypePublisher = "publisher"
	BrokerTypeConsumer  = "consumer"
)

func runBrokerManager(opts ...*brokerConfig) error {
	for _, opt := range opts {
		if opt.Enable {
			switch opt.Type {
			case BrokerTypePublisher:
				q, err := queue.NewPublisher(opt.Driver, opt.Dsn)
				if err != nil {
					return fmt.Errorf("glib: broker create publisher (%s) err: %v", opt.Alias, err)
				}
				if err = q.(queue.Conn).Ping(); err != nil {
					return fmt.Errorf("glib: broker resource (%s) err: %v", opt.Alias, err)
				}
				publishers.Store(opt.Alias, q)
			case BrokerTypeConsumer:
				q, err := queue.NewConsumer(opt.Driver, opt.Dsn)
				if err != nil {
					return fmt.Errorf("glib: broker create consumer (%s) err: %v", opt.Alias, err)
				}
				if err = q.(queue.Conn).Ping(); err != nil {
					return fmt.Errorf("glib: broker resource (%s) err: %v", opt.Alias, err)
				}
				consumers.Store(opt.Alias, q)
			default:
				return fmt.Errorf("glib: invalid broker type(%s)", opt.Type)
			}

		}
	}
	return nil
}

func closeBroker() {
	publishers.Range(func(key, value interface{}) bool {
		value.(queue.Publisher).Close()
		return true
	})
	consumers.Range(func(key, value interface{}) bool {
		value.(queue.Consumer).Close()
		return true
	})
}

func Publisher(alias string) queue.Publisher {
	eg, ok := publishers.Load(alias)
	if !ok {
		panic(fmt.Errorf("glib: broker publisher[%s] not configed", alias))
	}
	return eg.(queue.Publisher)
}

func Consumer(alias string) queue.Consumer {
	eg, ok := consumers.Load(alias)
	if !ok {
		panic(fmt.Errorf("glib: broker consumer[%s] not configed", alias))
	}
	return eg.(queue.Consumer)
}
