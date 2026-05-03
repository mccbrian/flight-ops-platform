package com.flightops.ingestion.controller;

import com.flightops.ingestion.dto.FlightOperationRequest;
import com.flightops.ingestion.service.FlightOperationIngestionService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RequiredArgsConstructor
@RequestMapping("/api/v1/flight-operations")
@RestController
public class FlightOperationController {

    private final FlightOperationIngestionService service;

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ResponseEntity<Void> ingest(@RequestBody FlightOperationRequest request) {
        service.ingest(request);
        return ResponseEntity.accepted().build();
    }

}
