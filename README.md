# KafkaConsumerAutoTune: The Intelligent Kafka Consumer

Welcome to **KafkaConsumerAutoTune**, a reference implementation of a high-performance Kafka consumer for Spring Boot. More than just a consumption tool, KafkaConsumerAutoTune is designed as a self-adaptive system capable of optimizing its own performance in real-time.

---

## Why KafkaConsumerAutoTune?

Traditionally, configuring a Kafka consumer is a guessing game:
- *How many messages should I take per batch (`max.poll.records`)?*
- *How long should I wait for the server (`fetch.max.wait.ms`)?*
- *How many threads do I need?*
- *How can I quickly generate test data that matches my complex business logic diagrams?*

If you set these values too low, you underutilize your resources. If you set them too high, you risk saturating your database or triggering incessant Kafka rebalances (the infamous "rebalance storm").

**KafkaConsumerAutoTune solves this by automating these settings and simplifying traffic generation.**

---

## Innovation: The "Cruise Control" (PID Controller)

Imagine you're driving a car. To maintain a constant speed, you don't press the accelerator at a fixed position. You adjust your pressure based on the slope, the wind, and the current speed.

KafkaConsumerAutoTune does exactly the same for Kafka using a **PID Controller** (Proportional, Integral, Derivative):
1. **Measurement**: It observes the real time taken to process a batch of messages.
2. **Target**: It has an objective (e.g., process each batch in exactly 1.2 seconds).
3. **Action**: If processing is too fast, it increases the number of messages per batch. If it slows down (e.g., the database is tired), it reduces the load instantly.

> **Result**: Constant optimal throughput, regardless of load or network health.

---

## Industrial-Grade Resilience

Real-world data processing is chaotic. KafkaConsumerAutoTune is built to survive the most common failures:

### 1. The Circuit Breaker
If your Oracle database goes down, KafkaConsumerAutoTune doesn't persist blindly. It "cuts the power":
- The Circuit Breaker transitions to the **OPEN** state.
- The Kafka consumer is automatically **paused** to avoid losing messages or saturating error logs.
- It resumes on its own as soon as the database is healthy again.

### 2. Individual Fallback Mode
This is the "surgical" mode. If a batch of 100 messages fails because of a single corrupted message (a "Poison Message"):
- KafkaConsumerAutoTune doesn't reject the entire batch.
- It retries each message **one by one**.
- The 99 healthy messages are saved.
- The faulty message is isolated and sent to the **DLT** (Dead Letter Topic).

---

## Full Observability

You can't improve what you don't measure. KafkaConsumerAutoTune offers:
- **Real-Time Dashboard**: Visualize throughput (msg/s), Kafka lag, and Optimizer interventions.
- **Distributed Tracing**: Trace every message from Kafka to the database via OpenTelemetry and Jaeger.
- **Structured Logs**: JSON format (ECS) ready for ELK, with automatic trace ID injection.

---

## Quick Start

### Prerequisites
- Docker and Docker Compose
- Java 21 (if you want to compile locally)

### Launch the complete stack
```bash
docker-compose up -d
```
This launches: Kafka, Oracle XE, Prometheus, Jaeger, Grafana, Loki, and the KafkaConsumerAutoTune
application. Oracle XE takes a few minutes to initialise on first start; the application waits for
it via `wait-for-db.sh`.

### Accessing the tools

Application (port 8080):

| Page | URL | What it shows |
|---|---|---|
| Dashboard | [/](http://localhost:8080/) | Throughput, lag, JVM and database health |
| Optimizer | [/optimizer](http://localhost:8080/optimizer) | History of auto-tuning decisions and why each was made |
| Consumer groups | [/consumer-groups](http://localhost:8080/consumer-groups) | Group state, members, per-partition lag |
| DLT management | [/dlt-management](http://localhost:8080/dlt-management) | Failed messages, with retry / edit-and-retry / discard |
| Message viewer | [/message-viewer](http://localhost:8080/message-viewer) | Recent payloads, with configurable JsonPath blocks |
| Simulation | [/simulation](http://localhost:8080/simulation) | Traffic generator, including malformed and duplicate records |
| Metrics | [/metrics](http://localhost:8080/metrics) | All Micrometer meters with trend and threshold status |
| Settings | [/settings](http://localhost:8080/settings) | Live log-level changes |
| Architecture | [/architecture](http://localhost:8080/architecture) | C4 diagrams |

Supporting stack:

- **Grafana**: [http://localhost:3000](http://localhost:3000)
- **Prometheus**: [http://localhost:9090](http://localhost:9090)
- **Jaeger (traces)**: [http://localhost:16686](http://localhost:16686)
- **Prometheus scrape endpoint**: [http://localhost:8080/actuator/prometheus](http://localhost:8080/actuator/prometheus)

### Running locally without Docker

The `dev` profile uses an in-memory H2 database and expects a Kafka broker at
`${KAFKA_BOOTSTRAP_SERVERS:kafkadev:9092}`:

```bash
./mvnw spring-boot:run
```

### Running the tests

```bash
./mvnw verify
```

This runs the full suite, integration tests included. They use an embedded Kafka broker and H2, so
no Docker daemon is required.

---

## Learn more
- [Detailed Technical Documentation](DOCUMENTATION.md)
- [Configuration Reference](docs/configuration.md)
- [Error Management](docs/error-management.md)
- [Observability](docs/observability.md)
- [Contributing Guidelines](CONTRIBUTING.md)

---

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
