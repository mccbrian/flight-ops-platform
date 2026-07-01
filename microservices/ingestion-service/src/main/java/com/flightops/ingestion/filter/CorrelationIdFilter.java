package com.flightops.ingestion.filter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.jspecify.annotations.NonNull;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;


/**
 * A filter that ensures all incoming HTTP requests contain a correlation ID, which is used
 * for request tracking and logging. If a correlation ID is not provided in the request headers,
 * this filter generates a new one, adds it to the logging context (MDC), and includes it in the
 * response headers.
 * <p>
 * This class extends {@code OncePerRequestFilter}, guaranteeing that the filter logic is executed
 * only once per request within a single request thread.
 */
@Component
public class CorrelationIdFilter extends OncePerRequestFilter {

    public static final String CORRELATION_ID_HEADER = "X-Correlation-Id";
    public static final String MDC_CORRELATION_ID = "correlationId";

    /**
     * Filters incoming HTTP requests to ensure a correlation ID is present. If the correlation ID
     * is not provided in the request header, a new one is generated and added. The correlation ID
     * is also added to the logging context (MDC) and the response header.
     *
     * @param request the HTTP servlet request being processed
     * @param response the HTTP servlet response where headers can be added
     * @param filterChain the filter chain to pass the request and response to the next filter
     * @throws ServletException if an error occurs during request processing
     * @throws IOException if an input or output exception occurs
     */
    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            @NonNull HttpServletResponse response,
            @NonNull FilterChain filterChain
    ) throws ServletException, IOException {

        String correlationId = request.getHeader(CORRELATION_ID_HEADER);

        if (correlationId == null || correlationId.isBlank()) {
            correlationId = UUID.randomUUID().toString();
        }

        MDC.put(MDC_CORRELATION_ID, correlationId);
        response.setHeader(CORRELATION_ID_HEADER, correlationId);

        try {
            filterChain.doFilter(request, response);
        } finally {
            MDC.remove(MDC_CORRELATION_ID);
        }
    }

}