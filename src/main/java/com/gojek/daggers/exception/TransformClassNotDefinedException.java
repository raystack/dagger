package com.gojek.daggers.exception;

public class TransformClassNotDefinedException extends RuntimeException {
    public TransformClassNotDefinedException(String message) {
        super(message);
    }
}
