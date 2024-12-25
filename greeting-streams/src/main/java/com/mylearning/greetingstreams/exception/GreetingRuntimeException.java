package com.mylearning.greetingstreams.exception;

public class GreetingRuntimeException extends RuntimeException{
    public GreetingRuntimeException(Exception message){
        super(message);
    }
}
