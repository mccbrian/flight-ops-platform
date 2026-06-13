package com.flightops.processing.domain;

import lombok.Getter;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

@Getter
@Setter
@Table("flights")
public class FlightReference {
    @Id
    private Integer flightId;
}
