package com.flux_dev.kafka_demo.controller;

import com.flux_dev.kafka_demo.consumer.SensorDataConsumer;
import com.flux_dev.kafka_demo.model.SensorReading;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

@RestController //expose data via REST endpoints
@RequestMapping("/api/sensors")
@CrossOrigin(origins = "http://localhost:3000")
public class SensorDataController {
    private final SensorDataConsumer consumer;

    public SensorDataController(SensorDataConsumer consumer) {
        this.consumer = consumer;
    }
        @GetMapping("/latest")
        public ResponseEntity<Map<String, LinkedList<SensorReading>>> getLatestReadings() {
            return ResponseEntity.ok(new TreeMap<>(consumer.getLatestReadings()));
        }
}
