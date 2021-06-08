package io.odpf.dagger.core.exception;

/**
 * The class Exception if there is failure in Http.
 */
public class HttpFailureException extends RuntimeException {
    /**
     * Instantiates a new Http failure exception.
     *
     * @param message the message
     */
    public HttpFailureException(String message) {
        super(message);
    }
}
