package com.flightops.processing.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

@Table("flights")
public class FlightReference {
    @Id
    private Integer flightId;
}
