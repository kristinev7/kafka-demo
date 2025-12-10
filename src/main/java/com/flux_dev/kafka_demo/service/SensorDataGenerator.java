package com.flux_dev.kafka_demo.service;

import com.flux_dev.kafka_demo.model.SensorReading;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
@RequiredArgsConstructor
public class SensorDataGenerator {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final Random random = new Random();

    @Value("${generator.topic}")
    private String topic;

    @Value("${generator.enabled}")
    private boolean enabled;

    private static final String [] LOCATIONS = {
            "warehouse", "factory-floor", "office", "loading-dock", "outdoor"
    };

    @Scheduled(fixedRateString = "${generator.interval-ms}")
    public void generateData() {
        if(!enabled) return;

        try {
            SensorReading reading = createRandomReading();
            String json = objectMapper.writeValueAsString(reading);

            CompletableFuture<SendResult<String, String>> future =
                kafkaTemplate.send(topic, reading.getSensorId(), json);

                future.whenComplete((result, ex) ->{
                    if (ex == null) {
                        log.info("Sent: {}", json);
                    } else {
                        log.error("Failed to send message", ex);
                    }
                });

            } catch(JsonProcessingException e) {
                log.error("Error serializing sensor data", e);
            }
    }



    private SensorReading createRandomReading() {
        SensorReading reading = new SensorReading();
        reading.setSensorId("sensor-" + (random.nextInt(5) + 1));
        reading.setTemperature(20 + random.nextDouble() *10);
        reading.setHumidity(30 + random.nextDouble() * 50);
        reading.setPressure(1000 + random.nextDouble() * 25);
        reading.setLocation(LOCATIONS[random.nextInt(LOCATIONS.length)]);
        reading.setTimestamp(Instant.now());
        return reading;
    }

}
