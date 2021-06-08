package io.odpf.dagger.functions.exceptions;

/**
 * The class Exception if Key does not exist.
 */
public class KeyDoesNotExistException extends RuntimeException {
    /**
     * Instantiates a new Key does not exist exception.
     *
     * @param message the message
     */
    public KeyDoesNotExistException(String message) {
        super(message);
    }
}
