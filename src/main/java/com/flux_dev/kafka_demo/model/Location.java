package com.flux_dev.kafka_demo.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Location {
    private final String id;
    private final LocationType type;

}
