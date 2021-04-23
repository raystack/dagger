package io.odpf.dagger.core.exception;

public class GrpcFailureException extends RuntimeException {
    public GrpcFailureException(String message) {
        super(message);
    }
}
