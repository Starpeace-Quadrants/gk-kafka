package kafka

import (
	"context"
	"github.com/segmentio/kafka-go"
	"log"
	"time"
)

func produce(message string, topic Topic, writeDeadline time.Time) {
	conn, err := kafka.DialLeader(context.Background(), "tcp", topic.Leader, topic.Topic, topic.Partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	if err := conn.SetWriteDeadline(writeDeadline); err != nil {
		log.Fatal("failed to set write deadline")
	}
	_, err = conn.WriteMessages(
		kafka.Message{Value: []byte(message)},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	if err := conn.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}
