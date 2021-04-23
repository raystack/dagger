package io.odpf.dagger.core.exception;

public class HttpFailureException extends RuntimeException {
    public HttpFailureException(String message) {
        super(message);
    }
}
