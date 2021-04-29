package io.odpf.dagger.core.exception;

public class ConsumerLagNotZeroException extends RuntimeException {
    public ConsumerLagNotZeroException(String message) {
        super(message);
    }
}
