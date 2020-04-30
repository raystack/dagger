package com.gojek.daggers.exception;

public class InvalidColumnMappingException extends RuntimeException {
    public InvalidColumnMappingException(String message) {
        super(message);
    }

    public InvalidColumnMappingException(String message, Throwable err) {
        super(message, err);
    }
}
