package kafka

import (
	"fmt"
	"github.com/segmentio/kafka-go"
)

func FetchTopics(host string, port int) ([]Topic, error) {
	conn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return nil, err
	}

	for _, p := range partitions {
		if len(p.Leader.Host) == 0 {
			p.Leader.Host = host
		}
		topics = append(topics, Topic{
			Topic:     p.Topic,
			Leader:    fmt.Sprintf("%s:%d", p.Leader.Host, p.Leader.Port),
			Partition: p.ID,
		})
	}

	return topics, nil
}
