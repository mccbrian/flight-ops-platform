package com.flightops.contracts.ingestion;

import java.io.Serializable;
import java.time.Instant;

public record FlightOperationEvent (

     Integer flightId,
     String operationType, // DELAY, GATE_CHANGE, etc.
     String status,
     String gate,
     Instant eventTime

) implements Serializable {}
