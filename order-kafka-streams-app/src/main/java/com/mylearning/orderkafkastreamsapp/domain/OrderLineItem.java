package com.mylearning.orderkafkastreamsapp.domain;

import java.math.BigDecimal;

public record OrderLineItem(
        String item,
        Integer count,
        BigDecimal amount) {
}
