package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/segmentio/kafka-go"
)

type AggregatedEvent struct {
	Color           string `json:"color"`
	WindowCount     int    `json:"window_count"`
	CumulativeCount int64  `json:"cumulative_count"`
}

const (
	topic   = "output-color-stats"
	groupID = "color-display-group"
)

func main() {
	brokerAddress := os.Getenv("KAFKA_BROKER")
	if brokerAddress == "" {
		brokerAddress = "localhost:9092"
	}

	log.Printf("Starting subscriber, connecting to Kafka broker: %s", brokerAddress)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{brokerAddress},
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	defer func() {
		if err := reader.Close(); err != nil {
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

	state := make(map[string]AggregatedEvent)

	clearScreen()
	fmt.Println("Start")

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				fmt.Println("\nClosing subscriber")
				return
			}
			log.Printf("Error reading message: %v", err)
			continue
		}

		var event AggregatedEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("Error parsing JSON: %v", err)
			continue
		}

		state[event.Color] = event

		drawDashboard(state)
	}
}

func clearScreen() {
	fmt.Print("\033[H\033[2J")
}

func drawDashboard(state map[string]AggregatedEvent) {
	clearScreen()
	fmt.Println(strings.Repeat("=", 50))
	fmt.Println("   REAL-TIME COLOR DASHBOARD")
	fmt.Println(strings.Repeat("=", 50))
	fmt.Printf("%-15s | %-15s | %-15s\n", "COLOR", "WINDOW COUNT", "CUMULATIVE")
	fmt.Println(strings.Repeat("-", 50))

	for _, colorEvent := range state {
		fmt.Printf("%-15s | %-15d | %-15d\n",
			strings.ToUpper(colorEvent.Color),
			colorEvent.WindowCount,
			colorEvent.CumulativeCount,
		)
	}
	fmt.Println(strings.Repeat("=", 50))
}
