package com.gojek.daggers.exception;

public class InvalidHttpVerbException extends RuntimeException {
    public InvalidHttpVerbException(String message) {
        super(message);
    }
}
