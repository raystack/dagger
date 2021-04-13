package io.odpf.dagger.exception;

public class GrpcFailureException extends RuntimeException {
    public GrpcFailureException(String message) {
        super(message);
    }
}
