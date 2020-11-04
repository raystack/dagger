package com.gojek.daggers.exception;

public class InvalidGrpcBodyException extends RuntimeException  {

    public InvalidGrpcBodyException(String message) {
        super(message);
    }
}
