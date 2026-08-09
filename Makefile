export KAFKA_PORT = 9092
export FLINK_UI_PORT = 8081
export FLINK_TASK_SLOTS = 2

INPUT_TOPIC = input-color-events
OUTPUT_TOPIC = output-color-stats
KAFKA_BROKER = localhost:$(KAFKA_PORT)
KAFKA_CONTAINER = kafka

.PHONY: up down run-publisher setup-topics 

run:
	$(MAKE) down
	$(MAKE) up
	$(MAKE) wait-for-kafka
	$(MAKE) setup-topics
	$(MAKE) run-flink

up:
	docker-compose up -d

down:
	docker-compose down -v

run-publisher:
	cd publisher && go run main.go

run-subscriber:
	cd subscriber && go run main.go

rebuild:
	docker-compose build --no-cache

setup-topics:
	docker exec $(KAFKA_CONTAINER) kafka-topics --create --if-not-exists --topic $(INPUT_TOPIC) --bootstrap-server $(KAFKA_BROKER) --partitions 1 --replication-factor 1
	docker exec $(KAFKA_CONTAINER) kafka-topics --create --if-not-exists --topic $(OUTPUT_TOPIC) --bootstrap-server $(KAFKA_BROKER) --partitions 1 --replication-factor 1

wait-for-kafka:
	@while ! docker exec $(KAFKA_CONTAINER) kafka-topics --list --bootstrap-server $(KAFKA_BROKER) > /dev/null 2>&1; do \
		sleep 3; \
	done

run-flink:
	docker exec -it flink-jobmanager flink run --python /app/main.py

logs-publisher:
	docker logs -f golang-publisher

logs-subscriber:
	docker logs -f golang-subscriber