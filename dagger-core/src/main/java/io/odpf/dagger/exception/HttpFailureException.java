package io.odpf.dagger.exception;

public class HttpFailureException extends RuntimeException {
    public HttpFailureException(String message) {
        super(message);
    }
}
