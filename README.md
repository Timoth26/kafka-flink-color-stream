# Real-Time Color Stream Processor (Kafka + PyFlink + Golang)

This project demonstrates a real-time event-driven architecture using **Apache Kafka** as the message broker, **Apache Flink (PyFlink)** for stateful stream processing, and **Golang** for producing and consuming events.

## Architecture Overview

The pipeline consists of four main components:
1. **Golang Publisher:** Generates random color events (`color`, `count`, `timestamp`) and publishes them to the Kafka topic `input-color-events`.
2. **Apache Kafka (Confluent 7.3.0):** Acts as a durable, decoupled message broker.
3. **PyFlink Job (DataStream API):** Consumes the raw events, applies Event-Time windowing and global state management, and outputs aggregated statistics.
4. **Golang Subscriber:** Consumes the aggregated statistics from the Kafka topic `output-color-stats` and displays them in the console.

---

## Quick Start Guide

### Prerequisites
- Docker & Docker Compose
- `make` utility installed
- Go 1.20+ (if Publisher/Subscriber are run locally outside Docker)

### Step-by-Step Deployment

**1. Clean up and build the infrastructure**
Ensure your Docker environment is clean.
```bash
make down
```

**2. Start the Cluster & Publisher**
Bring up Zookeeper, Kafka, and the Flink Cluster (JobManager & TaskManager). This step also creates the necessary Kafka topics.

```bash
make run
```

**3. Run the Publisher (Dashboard)**
Open a second terminal and start the Golang Publisher. You should see the Publisher logging emitted color events in this terminal

```bash
make run-publisher
```

**4. Run the Subscriber (Dashboard)**
Open a third terminal and start the Golang Subscriber to listen for processed results.

```bash
make run-subscriber
```

**5. Observe the Results**
Wait for the first 10-second window to close. In the Subscriber terminal, you will see real-time updates containing:\
window_count: The count of a specific color within the last 10 seconds.\
cumulative_count: The running total of a specific color since the job started.\
You can also monitor the job's DAG and metrics in the Flink Web UI at http://localhost:8081.