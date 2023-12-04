package consumer

import (
	"fmt"
	"github.com/IBM/sarama"
)

func InitializeKafkaConsumer() sarama.Consumer {
	brokerList := []string{"localhost:9092"}

	config := sarama.NewConfig()
	consumer, err := sarama.NewConsumer(brokerList, config)
	if err != nil {
		panic("Error initializing Kafka consumer: " + err.Error())
	}

	return consumer
}

func ConsumeEvents(consumer sarama.Consumer) {
	topic := "location"
	partition := int32(0)

	partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
	if err != nil {
		panic("Error creating partition consumer: " + err.Error())
	}
	defer partitionConsumer.Close()

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			// Deserialize the Avro-encoded message
			event, err := deserializeEvent(msg.Value)
			if err != nil {
				fmt.Println("Error deserializing event:", err)
				continue
			}

			// print the consumed event
			fmt.Printf("Consumed Event: %+v\n", event)

		case err := <-partitionConsumer.Errors():
			fmt.Println("Error:", err)

		}
	}
}
