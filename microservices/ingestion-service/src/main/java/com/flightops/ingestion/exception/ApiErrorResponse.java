package com.flightops.ingestion.exception;

import java.time.Instant;
import java.util.List;

public record ApiErrorResponse(
        String code,
        String message,
        String correlationId,
        String path,
        Instant timestamp,
        List<String> details
) {
    public static ApiErrorResponse of(
            String code,
            String message,
            String correlationId,
            String path
    ) {
        return new ApiErrorResponse(
                code,
                message,
                correlationId,
                path,
                Instant.now(),
                List.of()
        );
    }

    public static ApiErrorResponse of(
            String code,
            String message,
            String correlationId,
            String path,
            List<String> details
    ) {
        return new ApiErrorResponse(
                code,
                message,
                correlationId,
                path,
                Instant.now(),
                details
        );
    }
}