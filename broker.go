package glib

import (
	"fmt"
	"sync"

	"github.com/carltd/glib/queue"
	"github.com/micro/go-log"
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
					log.Logf("glib: broker (%s) %v", opt.Alias, err)
					continue
				}
				publishers.Store(opt.Alias, q)
			case BrokerTypeConsumer:
				q, err := queue.NewConsumer(opt.Driver, opt.Dsn)
				if err != nil {
					log.Logf("glib: broker (%s) %v", opt.Alias, err)
					continue
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
		panic(fmt.Errorf("glib: broker[%s] not configed", alias))
	}
	return eg.(queue.Publisher)
}

func Consumer(alias string) queue.Consumer {
	eg, ok := consumers.Load(alias)
	if !ok {
		panic(fmt.Errorf("glib: broker[%s] not configed", alias))
	}
	return eg.(queue.Consumer)
}
