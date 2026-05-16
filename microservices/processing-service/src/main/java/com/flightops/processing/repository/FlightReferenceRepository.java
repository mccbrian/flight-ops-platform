package com.flightops.processing.repository;

import com.flightops.processing.domain.FlightReference;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;

public interface FlightReferenceRepository extends CrudRepository<FlightReference, Integer> {

    @Query("SELECT EXISTS(SELECT 1 FROM flights WHERE flight_id = :flightId)")
    boolean existsByFlightId(@Param("flightId") Integer flightId);
}
