# Configuration Reference

Every property below has a default, so the application starts with none of them set. This page
exists because several of them are only defined in Java and never appear in `application.yml`,
which makes them invisible unless you read the source.

Defaults are taken from `KafkaTuningProperties`, `PersistenceProperties` and `KafkaConsumerConfig`.

---

## 1. Profiles

| Profile | Database | Kafka | Notes |
|---|---|---|---|
| `dev` (default) | H2 in-memory, `create-drop` | `${KAFKA_BOOTSTRAP_SERVERS:kafkadev:9092}` | H2 console at `/h2-console` |
| `local-h2` | H2 in-memory, `create-drop` | `localhost:9092` | For a broker running on the host |
| `rec` | Oracle, `validate` | SSL enabled | Requires the keystore/truststore variables below |
| `test` | inherits the base configuration | | |

The base configuration sets `spring.profiles.active: dev`, so **an unconfigured deployment runs
against an in-memory database that is discarded on restart**. Override it explicitly in production;
the standard `SPRING_PROFILES_ACTIVE` environment variable takes precedence over the YAML.

---

## 2. Auto-tuning (`kafka.tuning.*`)

The PID controller adjusts `max.poll.records` so that a batch takes `target-batch-duration-ms` to
process. Everything else is derived from the observed throughput and message size.

### Controller

| Property | Default | Meaning |
|---|---|---|
| `target-batch-duration-ms` | `1200.0` | The setpoint. Lower means smaller, more frequent batches. |
| `kp` | `150.0` | Proportional gain. Reacts to the current error. |
| `ki` | `20.0` | Integral gain. Removes steady-state offset. Clamped to ±10 internally to prevent windup. |
| `kd` | `50.0` | Derivative gain. Damps oscillation. |
| `ema-alpha` | `0.2` | Smoothing on the measured batch duration. Lower smooths more. |

The output is added directly to `max.poll.records`, so the gains are in "records per unit of
relative error" — an error of 1.0 (batches take twice the target) moves the batch size by roughly
`kp + ki + kd` records. Retune all three together.

### Bounds and safety

| Property | Default | Meaning |
|---|---|---|
| `min-max-poll-records` | `20` | Floor for `max.poll.records`. |
| `max-max-poll-records` | `1000` | Ceiling. Values above 1000 will break the idempotency lookup on Oracle unless the chunk size in `EventPersistenceService` is raised too. |
| `min-fetch-min-bytes` | `1024` | Floor for `fetch.min.bytes`. |
| `max-fetch-min-bytes` | `1048576` | Ceiling for `fetch.min.bytes`. |
| `fetch-max-bytes-safety-factor` | `1.5` | `fetch.max.bytes` is sized as `max.poll.records × avg message size × this`, floored at 1 MB. |
| `max-poll-interval-safety-factor` | `3.0` | `max.poll.interval.ms` is sized as `target duration × this`, floored at 30 s. |
| `change-threshold` | `0.1` | A parameter is only changed when the new value differs by at least 10%, or when it hits a bound. Prevents restarts for trivial adjustments. |
| `min-restart-interval-ms` | `300000` | Cooldown between consumer restarts, guarding against rebalance storms. See below. |

### Scaling

| Property | Default | Meaning |
|---|---|---|
| `cpu-threshold-high` | `0.8` | Above this system CPU load, concurrency is reduced by one. |
| `cpu-threshold-low` | `0.4` | Below this, **and** with lag above the threshold, concurrency is increased by one. |
| `lag-threshold-for-scaling` | `500` | Consumer lag required before scaling up. |
| `min-concurrency` | `1` | Floor for listener threads. Concurrency never exceeds the topic's partition count. |

### Scheduling

| Property | Default | Meaning |
|---|---|---|
| `fixed-rate` | `60000` | How often the tuning loop runs. |
| `initial-delay` | `30000` | Delay before the first run, letting metrics accumulate. |

### How the cooldown interacts with changes

Applying any parameter requires restarting the listener container, so changes are gated by
`min-restart-interval-ms`. When a change is proposed inside the cooldown window it is **discarded,
not queued**: nothing is written to the consumer factory and no internal state moves. The next
cycle recomputes from current measurements and proposes again. This means a change can be deferred
for up to one cooldown period, and that the values reported by the dashboard and the
`kafkaTuning` health indicator always describe what the consumer is actually running.

Emergency throttling (below) is subject to the same cooldown. Under sustained CPU or memory
saturation the protective reduction can therefore be deferred by up to `min-restart-interval-ms`.

### Emergency throttling

Not configurable. When system CPU load or heap usage exceeds 90%, the proposed `max.poll.records`
is cut by 30% (floored at `min-max-poll-records`) before any other sizing is derived from it.

---

## 3. Listener (`spring.kafka.listener.*`)

| Property | Default | Meaning |
|---|---|---|
| `concurrency` | `6` | Initial listener threads. Auto-tuning adjusts this at runtime; values above the partition count leave threads idle. |
| `retry.back-off-ms` | `2000` | Delay between redelivery attempts after a batch failure. |
| `retry.max-attempts` | `3` | Redelivery attempts before the batch is given up on. |

`PermanentException` and `JsonProcessingException` are classified as non-retryable: the consumer
already routes those to the DLT itself, so redelivering them only replays a message that cannot
succeed.

---

## 4. Topics and consumption

| Property | Default | Meaning |
|---|---|---|
| `kafka.topic.name` | `demo.app.topic` | Source topic. |
| `kafka.topic.dlt` | `demo.app.topic.dlt` | Dead letter topic. |
| `spring.kafka.consumer.group-id` | `DOMAIN.APP.MYID` | The DLT monitoring consumer uses this with a `-dlt` suffix. |
| `spring.kafka.consumer.auto-offset-reset` | `earliest` | |
| `spring.kafka.consumer.enable-auto-commit` | `false` | Offsets are acknowledged manually after persistence. |
| `spring.kafka.consumer.isolation-level` | `read_committed` | |
| `app.event.id-json-path` | `$.idPassage` | JsonPath used to extract the business key from the payload. A record whose key cannot be extracted goes to the DLT. |

`max-poll-records`, `fetch-min-bytes`, `fetch-max-wait-ms` and `max-poll-interval-ms` under
`spring.kafka.consumer.*` set the **starting** values only; auto-tuning overwrites them at runtime.

---

## 5. Persistence (`app.persistence.*`)

| Property | Default | Meaning |
|---|---|---|
| `save-in-file` | `false` | When true, events are written to disk instead of the database. The circuit breaker and idempotency check are bypassed in this mode. |
| `trace-path` | `trace` | Target directory, created at startup. |
| `format` | `json` | `json` or `xml`. |

Files are named `<topic>_<partition>_<offset>.<ext>`. The partition is part of the name because
offsets are only unique within a partition.

---

## 6. Metric thresholds (`app.metrics.thresholds.*`)

Each key is a **meter name**, mapped to `warning` and `critical` values. They drive the status
badges on the metrics page, and `kafka.lag` additionally drives the `kafkaLag` health indicator.

```yaml
app:
  metrics:
    thresholds:
      "kafka.lag":
        warning: 1000
        critical: 5000
      "app.kafka.events.received.count":
        warning: 100000
        critical: 500000
```

Application meter names are prefixed `app.` — see [Observability](observability.md) for the full
list. A key that does not match a real meter name is silently ignored, so check the name against
`/actuator/prometheus` when adding one.

---

## 7. SSL (`rec` profile)

| Variable | Purpose |
|---|---|
| `KAFKA_SSL_TRUSTSTORE_LOCATION` / `KAFKA_SSL_TRUSTSTORE_PASSWORD` | Broker trust material |
| `KAFKA_SSL_KEYSTORE_LOCATION` / `KAFKA_SSL_KEYSTORE_PASSWORD` / `KAFKA_SSL_KEY_PASSWORD` | Client certificate |
| `KAFKA_REC_BOOTSTRAP_SERVERS` | Broker list |

Applied consistently to the consumer, producer and AdminClient.

---

## 8. Database

| Variable | Purpose |
|---|---|
| `DB_HOST`, `DB_SERVICE_NAME` | Oracle connection target |
| `DB_USER`, `DB_PASSWORD` | Credentials, with no default — the application will not start without them on the Oracle profile |

The Oracle profile runs with `ddl-auto: validate`, so the schema must match the entities exactly.
See the schema notes in [DOCUMENTATION.md](../DOCUMENTATION.md#7-database-schema).
