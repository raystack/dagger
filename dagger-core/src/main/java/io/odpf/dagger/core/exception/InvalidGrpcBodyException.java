package io.odpf.dagger.core.exception;

/**
 * The class Exception if there is an Invalid Grpc body.
 */
public class InvalidGrpcBodyException extends RuntimeException {

    /**
     * Instantiates a new Invalid grpc body exception.
     *
     * @param message the message
     */
    public InvalidGrpcBodyException(String message) {
        super(message);
    }
}
