package com.gojek.daggers.exception;

public class GrpcFailureException extends RuntimeException {
    public GrpcFailureException(String message) {
        super(message);
    }
}
