package com.flightops.ingestion.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.flightops.ingestion.dto.FlightOperationRequest;
import com.flightops.ingestion.service.FlightOperationIngestionService;
import com.flightops.ingestion.utility.CamelCaseFormatter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.util.ResourceUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(FlightOperationController.class)
@DisplayNameGeneration(CamelCaseFormatter.class)
@DisplayName("Flight Operation Controller")
public class FlightOperationControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private FlightOperationIngestionService service;

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("uploadRequests")
    void whenIngestingFlightOperationEventsTheIngestMethodShould(
            String description,
            String jsonFile,
            HttpStatus expectedStatus) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        String json = readJsonFromFile(jsonFile);
        FlightOperationRequest request = objectMapper.readValue(json, FlightOperationRequest.class);

        when(service.ingest(any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        mockMvc.perform(MockMvcRequestBuilders.post("/api/v1/flight-operations")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().is(expectedStatus.value()));
    }

    private static Stream<Arguments> uploadRequests() {
        return Stream.of(
                Arguments.of(
                        Named.of(
                                "return a 202 for valid requests",
                                "return a 202 for valid requests"
                        ),
                        "subject/valid-event.json",
                        HttpStatus.ACCEPTED),
                Arguments.of(
                        Named.of(
                                "return a 400 when required fields are missing",
                                "return a 400 when required fields are missing"
                        ),
                        "subject/invalid-event.json",
                        HttpStatus.BAD_REQUEST)
        );
    }

    private static String readJsonFromFile(String filePath) throws IOException {
        return new String(Files.readAllBytes(Paths.get(ResourceUtils.getFile("classpath:" + filePath).toURI())));
    }

}