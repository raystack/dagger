package io.odpf.dagger.core.exception;

/**
 * The class Exception if there is failure in Grpc.
 */
public class GrpcFailureException extends RuntimeException {
    /**
     * Instantiates a new Grpc failure exception.
     *
     * @param message the message
     */
    public GrpcFailureException(String message) {
        super(message);
    }
}
