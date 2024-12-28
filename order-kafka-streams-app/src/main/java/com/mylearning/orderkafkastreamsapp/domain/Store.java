package com.mylearning.orderkafkastreamsapp.domain;

public record Store(String locationId,
                    Address address,
                    String contactNum) {
}
