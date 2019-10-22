package com.gojek.daggers.exception;

public class HttpFailureException extends RuntimeException {
    public HttpFailureException(String message) {
        super(message);
    }
}
