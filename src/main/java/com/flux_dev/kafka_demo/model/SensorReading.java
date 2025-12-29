package com.flux_dev.kafka_demo.model;

//automatically generates getters, setters, toString(), equals(), and hashCode() methods for a class
import lombok.Data;
//record event timestamps
import java.time.Instant;

@Data
public class SensorReading {
    private String sensorId;
    private Double temperature;
    private Double humidity;
    private Double pressure;
    private String locationId;
    private Instant timestamp;
    private LocationType locationType;
    private String location;

}
