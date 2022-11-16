package storage

import (
	"github.com/Shopify/sarama"
	"github.com/ronappleton/gk-kafka/storage"
)

type HasSaramaMessage interface {
	GetMessage(key string) sarama.ConsumerMessage
	SetMessage(message sarama.ConsumerMessage)
}

type EventData struct {
	storage.Storage
	HasSaramaMessage
}

func New() EventData {
	return EventData{}
}

func (e *EventData) GetMessage(key string) sarama.ConsumerMessage {
	return e.Get("message").(sarama.ConsumerMessage)
}

func (e *EventData) SetMessage(message sarama.ConsumerMessage) {
	e.Set("message", message)
}
