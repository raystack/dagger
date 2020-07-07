package com.gojek.daggers.exception;

public class RecordsNotConsumedException extends RuntimeException {
    public RecordsNotConsumedException(String message) {
        super(message);
    }
}
