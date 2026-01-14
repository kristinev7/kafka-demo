# Real-Time Event Streaming & Monitoring Platform

A **real-time, event-driven data streaming platform** built with **Apache Kafka** and **Spring Boot** to ingest, process, and expose continuous event data.
This project focuses on **distributed backend architecture**, **stream processing**, and a deliberate comparison between **polling-based** and **push-based** data delivery models. 
The system currently uses REST polling and WebSocket-based push delivery. 

---

## 🎯 Project Goals

- Build an **event-driven backend system** using Apache Kafka
- Process continuous event data with a scalable consumer architecture
- Expose streamed data via **REST APIs (polling)**
- Explore **push-based delivery using WebSockets** 
- Compare tradeoffs between polling and push delivery models
- Practice backend system design and data flow management

---

## 🏗️ Architecture Overview

### Event Producer
- Generates continuous event data (e.g., sensor readings)
- Publishes events to a Kafka topic
- Configurable event generation interval

### Messaging Layer
- Apache Kafka as the event backbone
- Single topic for event ingestion
- Supports decoupled producers and consumers

### Backend Consumer Service
- Spring Boot application
- Kafka consumer service for ingesting events
- Bounded in-memory retention of recent events
- Clear separation of consumer, service, and API layers

### Data Access & Delivery
- **REST APIs** for pull-based access to recent event data 
- **WebSocket (STOMP)** push delivery for real-time updates 

### Client Dashboard
- React-based frontend
- Displays recent event data
- Uses REST polling today
- Planned migration to WebSocket-based updates

---

## 🚀 Tech Stack

### Backend
- Java 17
- Spring Boot
- Apache Kafka
- Spring Kafka
- REST APIs
- WebSocket (STOMP) 

### Frontend [kafka-demo-frontend](https://github.com/kristinev7/kafka-demo-frontend)
- React
- REST client
- WebSocket client 

---

## 🔧 Core Features

- Event ingestion using Kafka producers and consumers
- Event-driven backend processing
- REST-based polling APIs for accessing recent events
- Bounded in-memory storage to limit resource usage
- Clear layering between ingestion, processing, and delivery
- WebSocket-based real-time delivery

---

## ⚖️ Polling vs Push Delivery (Design Focus)

This project intentionally explores two common approaches to delivering streaming data to clients.

### REST Polling 
- Clients request the latest event data at fixed intervals
- Simple to implement and widely supported
- Can result in higher latency and redundant requests

### WebSocket Push 
- Backend pushes updates to clients as soon as new events arrive
- Lower latency and more efficient for live dashboards
- Requires persistent connections and connection management


---

## 🔄 Data Flow

1. Event producer generates event data
2. Events are published to a Kafka topic
3. Spring Boot consumer ingests and processes events
4. Recent events are stored in memory with a fixed retention limit
5. Clients access data via:
   - REST polling 
   - WebSocket push delivery 
   
---

## 🧪 Local Setup

### Prerequisites
- Java 17+
- Apache Kafka (local or containerized)
- Maven

### Start Kafka
```bash
# Zookeeper
zookeeper-server-start.sh config/zookeeper.properties

# Kafka Broker
kafka-server-start.sh config/server.properties

---
Start the backend:  
```cd kafka-backend
./mvnw spring-boot:run
```
Backend runs at:  
`http://localhost:8080`

 ---
## Docker Setup 
```
# KRaft Mode  
docker compose -f docker-compose.kraft.yml --profile kraft up --build

# Zookeeper Mode (legacy)
docker compose -f docker-compose.zookeeper.yml --profile zookeeper up --build
```

 ---
 ## Start the Frontend:  [kafka-demo-frontend](https://github.com/kristinev7/kafka-demo-frontend)
 ```
cd kafka-demo-frontend
npm install
npm run dev
```

### 📌 Why This Project Matters

This project demonstrates:

- Event-driven backend architecture using Apache Kafka
- Real-world stream processing patterns
- Practical REST-based data delivery for monitoring use cases
- Intentional comparison of polling vs push-based systems
- Backend system design beyond simple CRUD applications

---
🧠 Notes  
This project prioritizes **architecture, data flow, and delivery tradeoffs** over domain-specific realism.  
The event data can represent telemetry from a wide range of systems.


