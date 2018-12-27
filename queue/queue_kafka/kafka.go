package queue_kafka

import (
	"errors"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/carltd/glib/queue"
	"log"
)

var (
	ErrNotSupport = errors.New("queue kafka: Not Support")
)

type kafkaQueueDriver struct{}

func (d *kafkaQueueDriver) OpenPublisher(addr string) (queue.Publisher, error) {
	info, err := parseURL(addr)
	if err != nil {
		return nil, err
	}
	log.Printf("[kafka] url is %s", info.Servers)
	log.Printf("[kafka] broker version is %s", info.BrokerVersion)

	var cfg = sarama.NewConfig()
	cfg.Version = info.BrokerVersion
	cfg.Producer.Return.Errors = true
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll

	client, err := sarama.NewSyncProducer(info.Servers, cfg)
	if err != nil {
		return nil, err
	}

	return &kafkaProducer{Producer: client}, nil
}

func (d *kafkaQueueDriver) OpenConsumer(addr string) (queue.Consumer, error) {
	info, err := parseURL(addr)
	if err != nil {
		return nil, err
	}
	log.Printf("[kafka] url is %s", info.Servers)
	log.Printf("[kafka] broker version is %s", info.BrokerVersion)

	cfg := cluster.NewConfig()
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Group.Return.Notifications = true
	cfg.Version = info.BrokerVersion
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	var ret = new(kafkaConsumer)
	ret.opts = cfg
	ret.servers = info.Servers
	return ret, nil
}

func init() {
	queue.Register("kafka", new(kafkaQueueDriver))
}
