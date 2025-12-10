package com.flux_dev.kafka_demo.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flux_dev.kafka_demo.model.SensorReading;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service //consumes data from Kafka
@Slf4j
@Getter
public class SensorDataConsumer {
    private final SimpMessagingTemplate messagingTemplate;
    private final ObjectMapper objectMapper;

    //Store last 50 readings for each sensor
    private final Map<String, LinkedList<SensorReading>> latestReadings = new ConcurrentHashMap<>();

    //DoubleSummaryStatistics calculates temperature's count, sum, min, max, average
    //ConcurrentHashMap is a thread-safe implementation of HashMap
    private final Map<String, DoubleSummaryStatistics> temperatureStats = new ConcurrentHashMap<>();

    public SensorDataConsumer(SimpMessagingTemplate messagingTemplate, ObjectMapper objectMapper) {
        this.messagingTemplate = messagingTemplate;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "${consumer.topic}")
    public void consume(String message) {
        try {
            SensorReading reading = objectMapper.readValue(message, SensorReading.class);
            //store to memory
            latestReadings.computeIfAbsent(reading.getSensorId(), k -> new LinkedList<>())
            .addFirst(reading);

            //trim to keep only latest 50 readings
            LinkedList<SensorReading> readings = latestReadings.get(reading.getSensorId());
            if (readings.size() > 50) {
                readings.removeLast();
            }
            //send reading to sensor-specific topic
            messagingTemplate.convertAndSend("/topic/sensor/" + reading.getSensorId(), reading);

            //send latest reading of each sensor to combined topic
            Map<String, SensorReading> latestData = new HashMap<>();
            latestReadings.forEach((sensorId, readingList) -> {
                if (!readingList.isEmpty()) {
                    latestData.put(sensorId, readingList.getFirst());
                }
            });
            messagingTemplate.convertAndSend("/topic/sensors/latest", latestData);
            log.info("Sent to WebSocket: {}", reading.getSensorId());
        } catch (IOException e) {
            log.error("Error deserializing message", e);
        }
    }

}
