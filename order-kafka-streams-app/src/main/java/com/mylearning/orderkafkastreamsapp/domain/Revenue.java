package com.mylearning.orderkafkastreamsapp.domain;

import java.math.BigDecimal;

public record Revenue(String locationId,
                      BigDecimal finalAmount) {
}
