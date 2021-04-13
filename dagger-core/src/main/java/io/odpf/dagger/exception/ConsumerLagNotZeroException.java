package io.odpf.dagger.exception;

public class ConsumerLagNotZeroException extends RuntimeException {
    public ConsumerLagNotZeroException(String message) {
        super(message);
    }
}
