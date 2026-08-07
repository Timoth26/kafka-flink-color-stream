INPUT_TOPIC = input-color-events
OUTPUT_TOPIC = output-color-stats
KAFKA_BROKER = localhost:9092
KAFKA_CONTAINER = kafka

.PHONY: up down run-publisher setup-topics 

up:
	docker-compose up -d

down:
	docker-compose down -v

run-publisher:
	cd publisher && go run main.go

setup-topics:
	docker exec $(KAFKA_CONTAINER) kafka-topics --create --if-not-exists --topic $(INPUT_TOPIC) --bootstrap-server $(KAFKA_BROKER) --partitions 1 --replication-factor 1
	docker exec $(KAFKA_CONTAINER) kafka-topics --create --if-not-exists --topic $(OUTPUT_TOPIC) --bootstrap-server $(KAFKA_BROKER) --partitions 1 --replication-factor 1