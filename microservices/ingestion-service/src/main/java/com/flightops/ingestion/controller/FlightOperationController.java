package com.flightops.ingestion.controller;

import com.flightops.ingestion.dto.FlightOperationRequest;
import com.flightops.ingestion.service.FlightOperationIngestionService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import jakarta.validation.Valid;
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

    @Operation(
            summary = "Ingest a flight operation event",
            description = "Accepts a flight operation request and publishes it to Kafka."
    )
    @ApiResponses({
            @ApiResponse(
                    responseCode = "202",
                    description = "Event accepted for processing"
            ),
            @ApiResponse(
                    responseCode = "400",
                    description = "Invalid request payload"
            )
    })
    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void ingest(@Valid @RequestBody FlightOperationRequest request) {
        service.ingest(request);
    }

}
