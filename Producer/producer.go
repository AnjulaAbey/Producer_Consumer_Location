package producer

import (
	"fmt"
	"github.com/IBM/sarama"
	"math/rand"
	"time"
)

// InitializeKafkaAsyncProducer  sets up a connection to a locally running Kafka instance using Sarama
func InitializeKafkaAsyncProducer() sarama.AsyncProducer {
	brokerList := []string{"localhost:9092"}

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Compression = sarama.CompressionSnappy

	producer, err := sarama.NewAsyncProducer(brokerList, config)
	if err != nil {
		panic("Error initializing Kafka async producer: " + err.Error())
	}
	return producer
}

// ProduceEvents generates and publishes events to the Kafka topic
func ProduceEvents(producer sarama.AsyncProducer) {
	topic := "location"

	for {
		event := Message{
			CustomerId: rand.Int63(),
			Latitude:   rand.Float64(),
			Longitude:  rand.Float64(),
		}

		avroData, err := SerializeEvent(event)
		if err != nil {
			fmt.Println("Error serializing event:", err)
			continue
		}

		// Publish the Avro serialized event to the Kafka
		producer.Input() <- &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(fmt.Sprintf("%d", event.CustomerId)),
			Value: sarama.ByteEncoder(avroData),
		}

		//print the event before publishing
		fmt.Printf("Published Event: %+v\n", event)

		// Wait for 10 seconds before producing the next event
		time.Sleep(10 * time.Second)
	}
}
