package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

type ColorEvent struct {
	Color     string `json:"color"`
	Count     int    `json:"count"`
	Timestamp int64  `json:"timestamp"`
}

const (
	brokerAddress = "localhost:9092"
	topic         = "input-color-events"
)

func main() {
	colors := []string{"red", "green", "blue", "yellow", "purple", "orange", "pink", "brown", "black", "white"}

	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokerAddress),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}

	defer func() {
		if err := writer.Close(); err != nil {
			log.Printf("Error closing Kafka connection: %v", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-signalChan
		cancel()
	}()

	fmt.Printf("Starting to send events to %s on topic %s...\n", brokerAddress, topic)

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Publisher has stopped.")
			return
		case <-ticker.C:
			randomColor := colors[rand.Intn(len(colors))]

			// Payload
			event := ColorEvent{
				Color:     randomColor,
				Count:     rand.Intn(5) + 1,
				Timestamp: time.Now().UnixMilli(),
			}

			eventBytes, err := json.Marshal(event)
			if err != nil {
				log.Printf("Error serializing JSON: %v", err)
				continue
			}

			msg := kafka.Message{
				Key:   []byte(randomColor),
				Value: eventBytes,
			}

			err = writer.WriteMessages(ctx, msg)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("Error while sending message: %v", err)
			} else {
				fmt.Printf("Sent: %s\n", string(eventBytes))
			}
		}
	}
}
