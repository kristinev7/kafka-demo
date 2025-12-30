package com.flux_dev.kafka_demo.model;

//automatically generates getters, setters, toString(), equals(), and hashCode() methods for a class
import lombok.Data;
//record event timestamps
import java.time.Instant;

@Data
public class SensorReading {
//    private String sensorId;
    private String locationId;
    private String location;
    private Double temperature;
    private Double humidity;
    private Double pressure;
    private Instant timestamp;
    private LocationType locationType;


}
