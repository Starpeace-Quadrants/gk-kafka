package events

import (
	"fmt"
	"github.com/ronappleton/gk-kafka/events/storage"
	"strings"
)

type Event struct {
	Tag  string
	Data storage.EventData
}

func NewEvent(tag string) *Event {
	return &Event{
		Tag:  tag,
		Data: storage.New(),
	}
}

func (e Event) String() string {
	return fmt.Sprintf("<Event %s with data %v>", e.Tag, e.Data)
}

type Events interface {
	Listen(prefix string) chan Event
	Announce(event Event)
	Signal(tag string)
}

type listener struct {
	prefix string
	sendon chan<- Event
}

type events struct {
	announcements chan Event
	listeners     chan listener
}

func New() Events {
	e := &events{
		announcements: make(chan Event),
		listeners:     make(chan listener),
	}
	go e.startListening()

	return e
}

func dispatch(c listener, e Event) (present bool) {
	defer func() {
		if r := recover(); r != nil {
			present = false
		}
	}()

	present = true
	if strings.HasPrefix(e.Tag, c.prefix) {
		c.sendon <- e
	}
	return
}

func (e *events) startListening() {
	var clients []listener
	for {
		select {
		case c := <-e.listeners:
			clients = append(clients, c)
		case e := <-e.announcements:
			for _, c := range clients {
				go dispatch(c, e)
			}
		}
	}
}

var globalEvents Events

func init() {
	globalEvents = New()
}

func (e *events) Listen(prefix string) chan Event {
	io := make(chan Event)
	e.listeners <- listener{prefix, io}
	return io
}

func (e *events) Announce(event Event) {
	e.announcements <- event
}

func (e *events) Signal(tag string) {
	e.Announce(Event{tag, storage.EventData{}})
}

func Listen(prefix string) chan Event {
	return globalEvents.Listen(prefix)
}

func Announce(event Event) {
	globalEvents.Announce(event)
}

func Signal(tag string) {
	globalEvents.Signal(tag)
}
