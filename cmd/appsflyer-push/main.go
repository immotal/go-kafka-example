package main

import (
	"log"
	"os"
	"os/signal"
	"strings"

	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"github.com/Shopify/sarama"
)

var (
	brokerList        = kingpin.Flag("brokerList", "List of brokers to connect").Default("kafka-cnngs4hyab3gkhlo.kafka.cn-beijing.ivolces.com:9092").Strings()
	// appsflyer push api 数据
	topic             = kingpin.Flag("topic", "Topic name").Default("appsflyer_push_event").String()
	groupID           = kingpin.Flag("group", "Consumer group id").Default("my-group-01").String() // 新增
	partition         = kingpin.Flag("partition", "Partition number").Default("0").String()
	offsetType        = kingpin.Flag("offsetType", "Offset Type (OffsetNewest | OffsetOldest)").Default("-1").Int()
	messageCountStart = kingpin.Flag("messageCountStart", "Message counter start from:").Int()
)

func main() {
	kingpin.Parse()
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	brokers := *brokerList
	master, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Panic(err)
	}
	defer func() {
		if err := master.Close(); err != nil {
			log.Panic(err)
		}
	}()
	consumer, err := master.ConsumePartition(*topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Panic(err)
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				log.Println(err)
			case msg := <-consumer.Messages():
				*messageCountStart++
				value := string(msg.Value)
				if strings.Contains(value, "af_regist_submit") {
				// if strings.Contains(value, "af_membership_select") {
					log.Println("Received messages", string(msg.Key), value)
				}
				// log.Println("Received messages", string(msg.Key), value)
			case <-signals:
				log.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()
	<-doneCh
	log.Println("Processed", *messageCountStart, "messages")
}
