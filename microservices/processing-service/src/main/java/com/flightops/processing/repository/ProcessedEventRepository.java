package com.flightops.processing.repository;

import com.flightops.processing.domain.ProcessedEvent;
import org.springframework.data.repository.CrudRepository;

import java.util.UUID;

public interface ProcessedEventRepository extends CrudRepository<ProcessedEvent, UUID> {
}