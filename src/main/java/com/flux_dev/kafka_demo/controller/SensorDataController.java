package com.flux_dev.kafka_demo.controller;

import com.flux_dev.kafka_demo.consumer.SensorDataConsumer;
import com.flux_dev.kafka_demo.model.SensorReading;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;

@RestController //expose data via REST endpoints
@RequestMapping("/api/sensors")
public class SensorDataController {
    private final SensorDataConsumer consumer;

    public SensorDataController(SensorDataConsumer consumer) {
        this.consumer = consumer;

    }
    @GetMapping("/latest")
    public ResponseEntity<Map<String, List<SensorReading>>> getLatestReadings() {
        Map<String, List<SensorReading>> out = new TreeMap<>();
        consumer.getLatestReadings().forEach((loc, deque) -> out.put(loc, new ArrayList<>(deque)));
        return ResponseEntity.ok(out);
    }
}
