package com.mylearning.orderkafkastreamsapp.exception;

public class OrderException extends RuntimeException{
    public OrderException(Exception e){
        super(e);
    }
}
