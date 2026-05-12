package com.flightops.processing.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;

@Table("flight_operation_status")
public class FlightOperationStatus {

    @Id
    private Integer flightId;

    private String status;
    private String gate;
    private Integer delayMinutes;
    private Instant updatedAt;

    public FlightOperationStatus(Integer flightId, String status, String gate, Integer delayMinutes) {
        this.flightId = flightId;
        this.status = status;
        this.gate = gate;
        this.delayMinutes = delayMinutes;
        this.updatedAt = Instant.now();
    }

    public void update(String status, String gate, Integer delayMinutes) {
        this.status = status;
        this.gate = gate;
        this.delayMinutes = delayMinutes;
        this.updatedAt = Instant.now();
    }

    public Integer getFlightId() { return flightId; }

}
