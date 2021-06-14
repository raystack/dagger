package io.odpf.dagger.core.exception;

/**
 * The class Exception if there is an Invalid Http verb.
 */
public class InvalidHttpVerbException extends RuntimeException {
    /**
     * Instantiates a new Invalid http verb exception.
     *
     * @param message the message
     */
    public InvalidHttpVerbException(String message) {
        super(message);
    }
}
