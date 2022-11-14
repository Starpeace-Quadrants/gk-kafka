package events

import "github.com/Shopify/sarama"

var MessageReceived messageReceived

type MessageReceivedPayload struct {
	Message sarama.ConsumerMessage
}

type messageReceived struct {
	handlers []interface {
		Handle(message MessageReceivedPayload)
	}
}

func (m *messageReceived) Register(handler interface {
	Handle(message MessageReceivedPayload)
}) {
	m.handlers = append(m.handlers, handler)
}

func (m messageReceived) Trigger(payload MessageReceivedPayload) {
	for _, handler := range m.handlers {
		go handler.Handle(payload)
	}
}
