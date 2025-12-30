package com.flux_dev.kafka_demo.service;

import com.flux_dev.kafka_demo.model.Location;
import com.flux_dev.kafka_demo.model.LocationType;
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
import java.util.List;
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

    private final List<Location> locations = List.of(
            new Location("warehouse-1", LocationType.WAREHOUSE),
            new Location("factory-floor-1", LocationType.FACTORY_FLOOR),
            new Location("office-1", LocationType.OFFICE),
            new Location("loading-dock-1", LocationType.LOADING_DOCK),
            new Location("outdoor-1", LocationType.OUTDOOR)
    );

    @Scheduled(fixedRateString = "${generator.interval-ms}")

    //one reading per location on each tick
    public void generateData() {
        if(!enabled) return;
        for (Location location : locations) {
            try {
                SensorReading reading = createReadingForLocation(location);
                String json = objectMapper.writeValueAsString(reading);

                //using sensorId as kafka key
                CompletableFuture<SendResult<String, String>> future =
                        kafkaTemplate.send(topic, reading.getLocationId(), json);

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

    }

    private SensorReading createReadingForLocation(Location location) {
        SensorReading reading = new SensorReading();
        // 1 sensor per location
        reading.setTemperature(generateTemperature(location.getType()));
        reading.setHumidity(generateHumidity(location.getType()));
        reading.setPressure(generatePressure(location.getType()));

        reading.setLocationId(location.getId());
        reading.setLocationType(location.getType());
        reading.setLocation(location.getId());
        reading.setTimestamp(Instant.now());
        return reading;
    }

    private double generateTemperature(LocationType type) {
        return switch (type) {
            case WAREHOUSE -> 5+random.nextDouble()*10;
            case FACTORY_FLOOR -> 18+random.nextDouble()*12;
            case OFFICE -> 20+random.nextDouble()*4;
            case LOADING_DOCK-> 10+random.nextDouble()*12;
            case OUTDOOR -> 5+random.nextDouble()*40;
        };
    }

    private double generateHumidity(LocationType type) {
        return switch (type) {
            case WAREHOUSE-> 55+random.nextDouble()*25;
            case FACTORY_FLOOR -> 35+random.nextDouble()*35;
            case OFFICE-> 30+random.nextDouble()*20;
            case LOADING_DOCK-> 40+random.nextDouble()*40;
            case OUTDOOR -> 20+random.nextDouble()*75;
        };
    }

    private double generatePressure(LocationType type) {
        double base = switch (type) {
            case OUTDOOR -> 995;
            default -> 1005;
        };
        return base + random.nextDouble()*15;
    }

}
