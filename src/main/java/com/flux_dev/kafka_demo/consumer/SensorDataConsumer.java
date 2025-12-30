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
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Function;

@Service //consumes data from Kafka
@Slf4j
@Getter
public class SensorDataConsumer {
    private static final int MAX_READINGS_PER_LOCATION = 50;
    private final SimpMessagingTemplate messagingTemplate;
    private final ObjectMapper objectMapper;

    //Store last 50 readings for each sensor
    //deque thread-safe per locaiton
    private final Map<String, Deque<SensorReading>> latestReadings = new ConcurrentHashMap<>();

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

            String locationKey = resolveLocationKey(reading);
            if (locationKey == null || locationKey.isBlank()) {
                log.warn("Dropping reading because location is missing. locationId={}, location={}",
                        reading.getLocationId(), reading.getLocation());
                return;
            }
            //store to memory latest 50 per location
            Deque<SensorReading> deque =
                    latestReadings.computeIfAbsent(locationKey, k -> new ConcurrentLinkedDeque<>());deque.addFirst(reading);

            //trim keep only 50 latest readings
            while (deque.size() > MAX_READINGS_PER_LOCATION) {
                deque.pollLast();
            }

            //send reading to location specific topic
            messagingTemplate.convertAndSend("/topic/location/" + locationKey, reading);

            //send latest reading of each location to combined topic
            Map<String, SensorReading> latestData = new HashMap<>();
            latestReadings.forEach((loc, readingDeque) -> {
                SensorReading first = readingDeque.peekFirst();
                if (first != null) {
                    latestData.put(loc, first);
                }
            });
            messagingTemplate.convertAndSend("/topic/locations/latest", latestData);
            log.debug("Sent to WebSocket: {}", locationKey);
        } catch (IOException e) {
            log.error("Error deserializing message", e);
        }
    }
    public DoubleSummaryStatistics getTemperatureStats(String location) {
        return computeStats(location, SensorReading::getTemperature);
    }
    public DoubleSummaryStatistics getHumidityStats(String location) {
        return computeStats(location, SensorReading::getHumidity);
    }
    public DoubleSummaryStatistics getPressureStats(String location) {
        return computeStats(location, SensorReading::getPressure);
    }
    private String resolveLocationKey(SensorReading reading) {
        if (reading.getLocationId() != null && !reading.getLocationId().isBlank()) {
            return reading.getLocationId();
        }
        return reading.getLocation();
    }

    private DoubleSummaryStatistics computeStats(String location, Function<SensorReading, Double> extractor){
        DoubleSummaryStatistics stats = new DoubleSummaryStatistics();
        Deque<SensorReading> readings = latestReadings.get(location);
        if (readings == null) {
            return stats;
        }
        for (SensorReading r : readings) {
            Double value = extractor.apply(r);
            if (value != null) {
                stats.accept(value);
            }
        }
        return stats;
    }


}
