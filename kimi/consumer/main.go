package main

import (
	"log"
	"os"
	"os/signal"

	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"github.com/Shopify/sarama"
)

var (
	brokerList        = kingpin.Flag("brokerList", "List of brokers to connect").Default("kafka-cnngsdeiyyw4eovl.kafka.ivolces.com:9093").Strings()
	// 小时级报表信息
	topic             = kingpin.Flag("topic", "Topic name").Default("oversea_group_hourly_report").String()
	username          = kingpin.Flag("user", "SASL username").Default("mart_kimi_datawarehouse").String()
	password          = kingpin.Flag("password", "SASL password").Default("kjoiugfdxSrtyuikjKoi4eT").String()
	groupID           = kingpin.Flag("group", "Consumer group id").Default("my-group-02").String() // 新增
	partition         = kingpin.Flag("partition", "Partition number").Default("0").String()
	offsetType        = kingpin.Flag("offsetType", "Offset Type (OffsetNewest | OffsetOldest)").Default("-1").Int()
	messageCountStart = kingpin.Flag("messageCountStart", "Message counter start from:").Int()
)

func main() {
	kingpin.Parse()
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// 配置 SASL 认证（使用你提供的 user / password）
	config.Net.SASL.Enable = true
	config.Net.SASL.User = *username
	config.Net.SASL.Password = *password
	config.Net.SASL.Mechanism = sarama.SASLTypePlaintext

	// 如服务端要求 TLS，可以取消下面两行注释，并按需要配置证书
	// config.Net.TLS.Enable = true
	// config.Net.TLS.Config = &tls.Config{InsecureSkipVerify: true}

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
	// consumer, err := master.ConsumePartition(*topic, 0, sarama.OffsetOldest)
	// consumer, err := master.ConsumePartition(*topic, 0, sarama.OffsetNewest)
	consumer, err := master.ConsumePartition(*topic, 0, sarama.OffsetOldest)
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
				log.Println("Received messages", string(msg.Key), string(msg.Value))
			case <-signals:
				log.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()
	<-doneCh
	log.Println("Processed", *messageCountStart, "messages")
}
