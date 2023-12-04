// main.go
package main

import (
	"github.com/IBM/sarama"
	"producer_consumer/Consumer"
	"producer_consumer/Producer"
)

func main() {
	kafkaProducer := producer.InitializeKafkaAsyncProducer()
	kafkaConsumer := consumer.InitializeKafkaConsumer()

	defer func(kafkaProducer sarama.AsyncProducer) {
		err := kafkaProducer.Close()
		if err != nil {
		}
	}(kafkaProducer)

	defer func(kafkaConsumer sarama.Consumer) {
		err := kafkaConsumer.Close()
		if err != nil {
		}
	}(kafkaConsumer)
	//running these two concurrently
	go producer.ProduceEvents(kafkaProducer)
	go consumer.ConsumeEvents(kafkaConsumer)

	select {}
}
