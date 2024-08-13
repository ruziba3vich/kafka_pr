package main

import (
	"encoding/json"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type User struct {
	Fullname string
	Phone    string
	Email    string
	Age      int
}

func main() {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	user := &User{
		Fullname: "John Doe",
		Phone:    "+1234567890",
		Email:    "johndoe@example.com",
		Age:      30,
	}

	topic := "users"

	byteData, err := json.Marshal(user)
	if err != nil {
		log.Fatal(err)
	}

	for range 5 {
		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Value: byteData,
		}, nil)
	}

	if err != nil {
		log.Fatal(err)
	}

	producer.Flush(15 * 1000)
}
