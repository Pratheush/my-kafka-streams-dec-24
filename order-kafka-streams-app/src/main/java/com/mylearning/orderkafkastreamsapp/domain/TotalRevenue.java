package com.mylearning.orderkafkastreamsapp.domain;

import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;

@Slf4j
public record TotalRevenue(String locationId,
                           Integer runningOrderCount,
                           BigDecimal runningRevenue) {

    // Default Constructor
    public TotalRevenue(){
        this("",0,BigDecimal.valueOf(0.0));
    }

    public TotalRevenue updateTotalRevenue(String locationId, Order order){
        log.info("New Record : key : {} , value : {} : ", locationId, order );
        var newOrderCount=this.runningOrderCount+1;
        var newRevenue=this.runningRevenue.add(order.finalAmount());
        return new TotalRevenue(locationId, newOrderCount, newRevenue);
    }

}
