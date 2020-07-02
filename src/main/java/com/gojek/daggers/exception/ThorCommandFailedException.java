package com.gojek.daggers.exception;

public class ThorCommandFailedException extends RuntimeException {
    public ThorCommandFailedException(String message) {
        super(message);
    }
}
