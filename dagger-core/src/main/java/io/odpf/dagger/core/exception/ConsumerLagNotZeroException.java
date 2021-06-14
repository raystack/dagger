package io.odpf.dagger.core.exception;

/**
 * The class Exception if kafka consumer lag is not zero.
 */
public class ConsumerLagNotZeroException extends RuntimeException {
    /**
     * Instantiates a new Consumer lag not zero exception.
     *
     * @param message the message
     */
    public ConsumerLagNotZeroException(String message) {
        super(message);
    }
}
