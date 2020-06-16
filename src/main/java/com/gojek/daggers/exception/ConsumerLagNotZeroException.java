package com.gojek.daggers.exception;

public class ConsumerLagNotZeroException extends RuntimeException {
    public ConsumerLagNotZeroException(String message) {
        super(message);
    }
}
