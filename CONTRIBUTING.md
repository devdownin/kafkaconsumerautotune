# Contributing to KafkaConsumerAutoTune

Thank you for your interest in contributing to **KafkaConsumerAutoTune**! We welcome contributions from the community to help make this project even better.

## Technical Stack

- **Language:** Java 21 (leveraging modern features like `record`).
- **Framework:** Spring Boot 3.5.9.
- **Build Tool:** Maven.
- **Database:** Oracle (Production-ready) or H2 (Development/Test).
- **Observability:** Prometheus, Micrometer, Jaeger (OpenTelemetry), Loki.
- **Infrastructure:** Docker & Docker Compose.

## How to Contribute

### 1. Reporting Bugs
- Use the GitHub Issue Tracker to report bugs.
- Provide a clear description of the issue and steps to reproduce it.

### 2. Suggesting Enhancements
- If you have an idea for a new feature or improvement, please open an issue first to discuss it.

### 3. Pull Requests
- Fork the repository.
- Create a new branch for your feature or bugfix (`git checkout -b feature/my-new-feature`).
- Ensure your code follows the existing style and conventions.
- **Tests are mandatory:** Include unit or integration tests for any new logic.
- Run tests locally using `./mvnw clean verify`.
- Submit a pull request with a detailed description of your changes.

## Key Areas of Interest

We are particularly interested in improvements in the following areas:
- **PID Controller Tuning:** Enhancements to the `KafkaTuningService` logic.
- **Resilience:** Improvements to the Circuit Breaker or Fallback mechanisms.
- **Observability:** New metrics, better tracing integration, or dashboard improvements.
- **Documentation:** Clarifying technical concepts or improving the Quick Start guide.

## Code of Conduct

Please be respectful and professional in all your interactions within this project.

## License

By contributing to this project, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).
