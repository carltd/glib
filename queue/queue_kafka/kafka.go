package queue_kafka

import (
	"github.com/Shopify/sarama"
	"log"

	"github.com/carltd/glib/queue"
)

type kafkaQueueDriver struct{}

func (d *kafkaQueueDriver) Open(addr string) (queue.Conn, error) {
	//info, err := parseURL(addr)
	//if err != nil {
	//	return nil, err
	//}
	log.Println(addr)

	var cfg = sarama.NewConfig()
	cfg.Version = sarama.V2_1_0_0
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll

	client, err := sarama.NewSyncProducer([]string{addr}, cfg)
	if err != nil {
		return nil, err
	}

	return &kafkaQueueConn{
		Addr:                []string{addr},
		Producer:            client,
		ReportErrors:        true, // TODO: config from dsn implement
		ReportNotifications: true, // TODO: config from dsn implement
		ServerVersion:       sarama.V2_1_0_0,
	}, nil
}

func init() {
	queue.Register("kafka", &kafkaQueueDriver{})
}
