# Release Notes

## v1.0.0

### Features

- :rocket: Initial release of the web analytics streaming pipeline
- :zap: Dual implementation of Apache Beam pipelines in Java and Python
- :clock1: Time spent calculation for user page views with session tracking
- :card_file_box: Support for multiple project categories:
  - Data batch processing pipelines
  - Data streaming pipelines
  - Application development (Flutter)
  - API services
  - Artificial Intelligence projects
- :whale: Containerized architecture with Docker Compose:
  - Kafka broker
  - Zookeeper
  - Event producer
  - Optional Kafka expansion service for Python
- :octocat: GitHub Actions CI/CD workflow integration
- :lock: Apache 2.0 license and proper documentation

### Technical Improvements

- :coffee: Java implementation using Apache Beam's native Kafka connector
- :snake: Python implementation using Apache Beam's external Kafka transforms
- :package: Maven and Poetry dependency management
- :chart_with_upwards_trend: Session-based window analytics for user engagement tracking
- :bookmark: UUID-based session identification

### Documentation

- :memo: Detailed README with usage instructions and project structure
- :gear: Configuration guidance for both Java and Python implementations
- :bulb: Kafka command references for topic management
- :art: Architecture diagrams using Mermaid syntax
