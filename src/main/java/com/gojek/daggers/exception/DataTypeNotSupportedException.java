package com.gojek.daggers.exception;

public class DataTypeNotSupportedException extends RuntimeException {
    public DataTypeNotSupportedException(String message) {
        super(message);
    }
}
