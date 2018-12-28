package queue_kafka

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/carltd/glib/queue/util"
)

type dialInfo struct {
	// Servers holds the addresses for the server.
	Servers []string

	BrokerVersion sarama.KafkaVersion
}

func parseURL(url string) (*dialInfo, error) {
	opt, err := util.ExtractURL(url)

	if err != nil {
		return nil, err
	}

	var (
		bVersion sarama.KafkaVersion
	)
	for k, v := range opt.Options {
		switch k {
		case "broker_version":
			if bVersion, err = sarama.ParseKafkaVersion(v); err != nil {
				return nil, fmt.Errorf("parse borker_version=%v : %v", v, err)
			}
		default:
			return nil, errors.New("unsupported connection URL option: " + k + "=" + v)
		}
	}

	info := dialInfo{
		Servers:       strings.Split(strings.TrimPrefix(opt.Addr, "kafka://"), ","),
		BrokerVersion: bVersion,
	}

	return &info, nil
}
