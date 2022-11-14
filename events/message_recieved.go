package events

import "github.com/Shopify/sarama"

var MessageReceived messageReceived

type MessageReceivedPayload struct {
	Some string
}

type messageReceived struct {
	handlers []interface {
		Handle(message sarama.ConsumerMessage)
	}
}

func (m *messageReceived) Register(handler interface {
	Handle(message sarama.ConsumerMessage)
}) {
	m.handlers = append(m.handlers, handler)
}

func (m messageReceived) Trigger(payload sarama.ConsumerMessage) {
	for _, handler := range m.handlers {
		go handler.Handle(payload)
	}
}
