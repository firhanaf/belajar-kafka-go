package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"os"
)

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "logs",
		GroupID: "consumer-group-A",
	})
	defer reader.Close()

	file, err := os.OpenFile("consumer.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal("failed to open file:", err)
	}
	defer file.Close()

	fmt.Println("Consumer started... waiting for messages")

	count := 0
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Println("failed to read message:", err)
			continue
		}
		count++
		logLine := fmt.Sprintf("[%s] Message received: %s \n", m.Time.Format("2006-01-02 15:04:05"), string(m.Value))
		fmt.Println(logLine)
		file.WriteString(logLine)
		fmt.Printf("Total message received: %d\n", count)
	}
}
