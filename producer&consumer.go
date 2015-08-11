package main

import (
	"fmt"
	"github.com/bitly/go-nsq"
	"strconv"
	"time"
)

func main() {
	producer()
	consumer()
}

func producer() {
	producer, err := nsq.NewProducer("m7:4150", nsq.NewConfig())
	defer producer.Stop()
	if err != nil {
		fmt.Println(err.Error())
	}
	for i := 0; i < 100000; i++ {
		err = producer.Publish("test", []byte("testing"+strconv.Itoa(i)))
		if err != nil {
			fmt.Println(err.Error())
		}
	}
}

func consumer() {
	consumer, err := nsq.NewConsumer("test", "abc", nsq.NewConfig())
	if err != nil {
		fmt.Println(err.Error())
	}
	handler := new(MyMessageHandler)
	handler.msgchan = make(chan *nsq.Message, 1024)
	consumer.AddHandler(nsq.HandlerFunc(handler.HandleMessage))
	err = consumer.ConnectToNSQLookupd("m7:4161")
	if err != nil {
		fmt.Println(err.Error())
	}
	handler.Process()
}

type MyMessageHandler struct {
	msgchan chan *nsq.Message
	stop    bool
}

func (m *MyMessageHandler) HandleMessage(message *nsq.Message) error {
	if !m.stop {
		m.msgchan <- message
	}
	return nil
}
func (m *MyMessageHandler) Process() {
	m.stop = false
	for {
		select {
		case message := <-m.msgchan:
			fmt.Println(string(message.Body))
		case <-time.After(time.Second):
			if m.stop {
				close(m.msgchan)
				return
			}
		}
	}
}
