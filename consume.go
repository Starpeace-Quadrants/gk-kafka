package kafka

import (
	"context"
	"github.com/segmentio/kafka-go"
	"log"
	"time"
)

func consume(topic Topic, readDeadline time.Time) ([]byte, error) {
	conn, err := kafka.DialLeader(context.Background(), "tcp", topic.Leader, topic.Topic, topic.Partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	if err := conn.SetReadDeadline(readDeadline); err != nil {
		log.Fatal("failed to set read deadline:", err)
	}

	msg, err := conn.ReadMessage(1e6) // fetch 1MB max
	if err != nil {
		log.Fatal("failed to read message:", err)

		return []byte{}, nil
	}

	return msg.Value, nil
}
