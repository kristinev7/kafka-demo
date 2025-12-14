# Kafka Sensor Data Streaming Demo

A real-time sensor data streaming application demonstrating Apache Kafka's capabilities with a Spring Boot backend 
and React frontend.
[Kafka-Demo-Frontend](https://github.com/kristinev7/kafka-demo-frontend)

## Overview

This project simulates real-world IoT data streaming by generating artificial sensor readings (temperature, humidity, pressure) and streaming them through Kafka. The data is consumed by a Spring Boot service and made available via REST API and WebSocket for real-time updates.

## Architecture

```
┌─────────────────┐      ┌─────────────┐      ┌────────────────────┐
│  Data Generator │─────▶│    Kafka    │─────▶│ SensorDataConsumer │
│  (produces)     │      │ sensor-data │      │ (consumes)         │
└─────────────────┘      └─────────────┘      └─────────┬──────────┘
                                                        │
                                          ┌─────────────┴─────────────┐
                                          ▼                           ▼
                                   ┌─────────────┐           ┌──────────────┐
                                   │  In-Memory  │           │  WebSocket   │
                                   │   Storage   │           │  /topic/*    │
                                   └──────┬──────┘           └──────────────┘
                                          │                         ▲
                                          ▼                         │
                                   ┌─────────────────┐              │
                                   │ REST Controller │              │
                                   │ /api/sensors/*  │              │
                                   └────────┬────────┘              │
                                            │                       │
                                            ▼                       │
                                   ┌─────────────────┐              │
                                   │    Frontend     │──────────────┘
                                   │  (React/Vite)   │  (real-time updates)
                                   └─────────────────┘
```

## Project Structure

```
kafka-demo/
└── src/main/java/com/flux_dev/kafka_demo/
    ├── config/
    │   ├── CorsGlobalConfig.java    # CORS settings for cross-origin requests
    │   ├── KafkaConfig.java         # Kafka topic configuration
    │   └── WebSocketConfig.java     # WebSocket/STOMP setup for real-time updates
    ├── consumer/
    │   └── SensorDataConsumer.java  # Kafka message consumer service
    ├── controller/
    │   └── SensorDataController.java # REST API endpoints
    ├── generator/
    │   └── DataGenerator.java       # Simulated sensor data producer
    └── model/
        └── SensorReading.java       # Data model for sensor readings
```

## Prerequisites

- Java 17+
- Apache Kafka (running on localhost:9092)
- Maven

## Getting Started

### 1. Start Kafka

Make sure Kafka and Zookeeper are running:

```bash
# Start Zookeeper
zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka
kafka-server-start.sh config/server.properties
```

### 2. Start the Backend

```bash
cd kafka-demo
./mvnw spring-boot:run
```

The backend will start on `http://localhost:8080`

## API Endpoints

### REST API

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/sensors/latest` | Get latest readings for all sensors (up to 50 per sensor) |

### WebSocket

| Endpoint | Description |
|----------|-------------|
| `/ws` | WebSocket connection endpoint (SockJS) |
| `/topic/sensors/latest` | Subscribe for real-time updates from all sensors |
| `/topic/sensor/{sensorId}` | Subscribe for updates from a specific sensor |

## Configuration

### application.properties

| Property | Default | Description |
|----------|---------|-------------|
| `spring.kafka.bootstrap-servers` | `localhost:9092` | Kafka broker address |
| `generator.topic` | `sensor-data` | Kafka topic for producing sensor data |
| `generator.interval-ms` | `1000` | Data generation interval in milliseconds |
| `generator.enable` | `true` | Enable/disable data generator |
| `consumer.topic` | `sensor-data` | Kafka topic to consume from |
| `spring.kafka.consumer.group-id` | `sensor-group` | Consumer group ID |
| `spring.kafka.consumer.auto-offset-reset` | `earliest` | Where to start reading if no offset exists |

### Kafka Topic Settings

The topic is automatically created on startup with:
- **Partitions:** 1 (increase for parallel processing across multiple consumers)
- **Replicas:** 1 (increase for fault tolerance; requires multiple Kafka brokers)

## Data Model

### SensorReading

```json
{
    "sensorId": "sensor-001",
    "temperature": 23.5,
    "humidity": 65.2,
    "pressure": 1013.25,
    "location": "Building A",
    "timestamp": "2024-01-15T10:30:00Z"
}
```

## Key Components

### SensorReading (Model)
Data class representing a single sensor measurement with fields for sensor ID, temperature, humidity, pressure, location, and timestamp.

### SensorDataConsumer (Service)
Listens to the Kafka `sensor-data` topic and:
- Deserializes JSON messages into `SensorReading` objects
- Stores the last 50 readings per sensor in memory (configurable)
- Broadcasts updates via WebSocket to connected clients

### SensorDataController (REST API)
Exposes the `/api/sensors/latest` endpoint for fetching sensor data on-demand.

### Config Classes
- **KafkaConfig:** Creates the Kafka topic with specified partitions and replicas
- **WebSocketConfig:** Configures STOMP over WebSocket for real-time push updates
- **CorsGlobalConfig:** Allows cross-origin requests from the frontend

---

## Dependencies

### Lombok

#### `@Data`
- Automatically generates getters, setters, toString(), equals(), and hashCode() methods for a class
- Saves you from writing repetitive code for data classes

#### `@RequiredArgsConstructor`
- Generates a constructor with required arguments for all final fields
- Useful for constructor dependency injection in Spring

#### `@Slf4j`
- Creates a logger instance in your class
- Adds a `log` field you can use for logging without manual setup

### Jackson

#### `ObjectMapper`
- The main Jackson class for converting Java objects to JSON and vice versa
- Used to serialize your Java objects to JSON strings before sending to Kafka

#### `JsonProcessingException`
- Exception thrown when processing JSON content fails
- Used for error handling during JSON serialization/deserialization

### Spring

#### `@EnableScheduling`
- Enables the scheduling capabilities
- Allows you to use @Scheduled annotations for running methods at fixed intervals

#### `@Component`
- Marks a class as a component
- Tells Spring to detect this class during component scanning and add it to the application context

#### `@PostConstruct`
- Specifies a method to run after dependency injection is complete
- Used to perform initialization logic after a bean's construction

### Summary

These imports provide functionality for:
- Object-to-JSON conversion (Jackson)
- Reducing boilerplate code (Lombok)
- Scheduling recurring tasks (Spring)
- Bean lifecycle management (Spring/JavaEE)

---

## Customization

### Changing the Reading History Limit

By default, the consumer stores the last 50 readings per sensor. To change this, modify `SensorDataConsumer.java`:

```java
// In the consume() method, change 50 to your desired limit
if (readings.size() > 50) {
    readings.removeLast();
}
```

Note: Data is stored in memory, so higher limits will increase RAM usage.

[//]: # (### Scaling for Production)

[//]: # ()
[//]: # (For production deployments, consider:)

[//]: # ()
[//]: # (1. **Increase Partitions** - Allows parallel consumption for higher throughput)

[//]: # (2. **Increase Replicas** - Provides fault tolerance &#40;requires multiple Kafka brokers&#41;)

[//]: # (3. **External Database** - Replace in-memory storage with a persistent database)

[//]: # (4. **Environment Variables** - Externalize configuration for different environments)


