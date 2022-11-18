package kafka

import (
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
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
		topics = append(topics, Topic{
			Topic:     p.Topic,
			Leader:    fmt.Sprintf("%s:%s", "kafka", "9092"),
			Partition: p.ID,
		})
	}

	log.Printf("Topic: %+v", topics)
	return topics, nil
}
