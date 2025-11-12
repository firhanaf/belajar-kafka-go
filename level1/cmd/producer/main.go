package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"time"
)

func main() {
	writer := kafka.Writer{
		Addr:     kafka.TCP("127.0.0.1:9092"),
		Topic:    "logs",
		Balancer: &kafka.LeastBytes{},
	}

	fmt.Println("Producer started... sending messages every 5 seconds")

	for {
		msg := fmt.Sprintf("Hello Kafka! Time: %s", time.Now().Format(time.RFC3339))
		err := writer.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte("key-A"),
			Value: []byte(msg),
		},
		)
		if err != nil {
			fmt.Println("failed to write messages:", err)
		} else {
			fmt.Println("sent:", msg)
		}
		time.Sleep(5 * time.Second)
	}

}
