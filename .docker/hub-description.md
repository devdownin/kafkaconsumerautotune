# KafkaConsumerAutoTune

**A Kafka consumer that tunes itself.** Instead of guessing `max.poll.records`
and `fetch.max.wait.ms` and living with the consequences, this Spring Boot
consumer measures its own throughput and adjusts its settings continuously,
while staying up when the database underneath it does not.

```bash
docker pull devdownin/kafkaconsumerautotune:latest
```

![The pipeline overview: throughput, success rate, consumer lag and the parameters the optimizer is currently using](https://raw.githubusercontent.com/devdownin/kafkaconsumerautotune/main/docs/images/dashboard.png)

---

## The problem it solves

Tuning a Kafka consumer is a guessing game. Set the batch size too low and you
waste the resources you are paying for. Set it too high and you saturate your
database — or trigger the rebalance storms that take a consumer group offline
for minutes at a time. The right value is not a constant anyway: it changes with
load, with network health, with how tired your database is at 3 a.m.

KafkaConsumerAutoTune stops treating it as a configuration problem and starts
treating it as a control problem.

## Cruise control for your consumer

Think of how you hold a constant speed in a car. You do not fix the accelerator
at one position — you adjust it against the slope and the wind. A **PID
controller** does the same here:

- **It measures** the real time taken to process each batch.
- **It targets** a processing time you choose — say, 1.2 seconds per batch.
- **It acts** — throughput too fast, it takes larger batches; slowing down, it
  backs off immediately.

The result is optimal throughput that holds regardless of load, instead of a
number someone picked during a sprint two years ago.

![The optimizer timeline: throughput against max.poll.records and concurrency, with every adjustment listed and explained](https://raw.githubusercontent.com/devdownin/kafkaconsumerautotune/main/docs/images/optimizer.png)

Every adjustment is recorded with the measurement that triggered it, so the
consumer's behaviour stays explainable rather than mysterious.

## Built to survive a bad day

**Circuit breaker.** When the database goes down, the consumer does not keep
hammering it. The breaker opens, the Kafka listener is paused — no lost
messages, no error logs filling your disk — and it resumes on its own once the
database is healthy.

**Surgical fallback.** One corrupted message should not cost you the other 99.
When a batch fails, each message is retried individually: the healthy ones are
persisted, and only the poison message is isolated and routed to the Dead
Letter Topic.

From there it is yours to handle: inspect the headers and the error, fix the
payload in place, and replay it — or discard it.

![Dead Letter Topic management: the failed messages, and the inspection panel showing headers, error trace and an editable payload](https://raw.githubusercontent.com/devdownin/kafkaconsumerautotune/main/docs/images/dlt-management.png)

## You can see what it is doing

- **Live dashboard** — throughput, Kafka lag per partition, and every
  intervention the optimizer makes, as it makes it.
- **Distributed tracing** — follow a single message from Kafka to the database
  through OpenTelemetry and Jaeger.
- **Structured logs** — JSON in ECS format, ready for your log stack, with
  trace IDs injected automatically.

---

## Quick start

The image expects a Kafka broker and a database. The fastest way to see it
running is the full stack from the repository:

```bash
git clone https://github.com/devdownin/kafkaconsumerautotune.git
cd kafkaconsumerautotune
docker compose up -d
```

That brings up Kafka, Oracle XE, Prometheus, Jaeger and the application. Then
open:

- Dashboard — <http://localhost:8080/dashboard>
- Kafka optimizer — <http://localhost:8080/optimizer>
- Traces — <http://localhost:16686>

The screenshots above show the interface with sample data; the figures in them
are illustrative.

To run the image on its own, point it at your own infrastructure:

```bash
docker run -p 8080:8080 \
  -e KAFKA_BOOTSTRAP_SERVERS=broker:9092 \
  -e DB_HOST=oracle -e DB_SERVICE_NAME=XEPDB1 \
  -e DB_USER=appuser -e DB_PASSWORD=... \
  devdownin/kafkaconsumerautotune:latest
```

## Configuration

| Variable | Purpose |
| --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | Broker addresses |
| `DB_HOST`, `DB_SERVICE_NAME` | Database host and service |
| `DB_USER`, `DB_PASSWORD` | Database credentials |
| `KAFKA_SSL_TRUSTSTORE_PASSWORD` | TLS truststore, when SSL is enabled |
| `KAFKA_SSL_KEYSTORE_PASSWORD`, `KAFKA_SSL_KEY_PASSWORD` | TLS keystore and key |
| `LOG_PATH` | Directory for the file log appender |
| `SPRING_PROFILES_ACTIVE` | `dev` for the embedded database, or your own profile |

No credentials are baked into the image — every one of them is read from the
environment.

## Tags

| Tag | Points to |
| --- | --- |
| `latest` | Most recent stable release |
| `1.0.1` | An exact release |
| `1.0`, `1` | Latest patch and latest minor of that line |
| `sha-<commit>` | One precise build, for pinning |

Pre-releases are published under their own version and never take `latest`.

## About the image

Built from a multi-stage Dockerfile, it runs as an **unprivileged user**,
exposes port **8080**, and ships a `HEALTHCHECK` against
`/actuator/health` so your orchestrator knows when it is genuinely ready.

Spring Boot 3.5 · Apache License 2.0

**Source, issues and full documentation:**
<https://github.com/devdownin/kafkaconsumerautotune>
