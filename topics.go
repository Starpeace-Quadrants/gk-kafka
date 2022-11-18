package kafka

import (
	"errors"
	"fmt"
	"strings"
)

type Topic struct {
	Topic     string
	Leader    string
	Partition int
}

var topics []Topic

func init() {
	InitTopics("kafka", 9092)
}

func GetTopics() []Topic {
	return topics
}

func GetTopicByName(topicName string, direction string) (Topic, error) {
	for _, topic := range topics {
		if strings.HasPrefix(topic.Topic, topicName) &&
			strings.HasSuffix(topic.Topic, fmt.Sprintf("_%s", direction)) {
			return topic, nil
		}
	}

	return Topic{}, errors.New("unable to find requested topic")
}

func GetReceiveTopics() []Topic {
	var outTopics []Topic
	for _, topic := range topics {
		if strings.HasSuffix(topic.Topic, "_out") {
			outTopics = append(outTopics, topic)
		}
	}

	return outTopics
}

func GetSendTopics() []Topic {
	var outTopics []Topic
	for _, topic := range topics {
		if strings.HasSuffix(topic.Topic, "_in") {
			outTopics = append(outTopics, topic)
		}
	}

	return outTopics
}

func InitTopics(host string, port int) {
	fetched, err := FetchTopics(host, port)
	if err != nil {
		panic(err)
	}

	topics = fetched
}
