package main

import (
	"fmt"
	"time"

	"github.com/IBM/sarama"
)

// TODO
// 1. add versions?
// 2. acks = saramaWaitForLocal?
// 3. change to consumer groups


func makeConsumer(kafkaAddr string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	var consumer sarama.Consumer
	var err error
	for i := 0; i < 10; i++ {
		consumer, err = sarama.NewConsumer([]string{kafkaAddr}, config)
		if err == nil {
			return consumer, nil
		}
		time.Sleep(5 * time.Second)
	}
	return nil, fmt.Errorf("failed to start consumer: %w", err)
}

func makeProducer(kafkaAddr string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	var prod sarama.SyncProducer
	var err error
	for i := 0; i < 10; i++ {
		prod, err = sarama.NewSyncProducer([]string{kafkaAddr}, config)
		if err == nil {
			return prod, nil
		}
		time.Sleep(5 * time.Second)
	}
	return nil, fmt.Errorf("failed to start producer: %w", err)
}