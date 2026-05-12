package com.flightops.processing.repository;

import com.flightops.processing.domain.FlightOperationStatus;
import org.springframework.data.repository.CrudRepository;

public interface FlightOperationStatusRepository extends CrudRepository<FlightOperationStatus, Integer> {
}