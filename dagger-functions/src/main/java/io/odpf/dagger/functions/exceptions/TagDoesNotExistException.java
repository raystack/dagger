package io.odpf.dagger.functions.exceptions;

/**
 * The class Exception if Tag does not exist.
 */
public class TagDoesNotExistException extends RuntimeException {
    /**
     * Instantiates a new Tag does not exist exception.
     *
     * @param message the message
     */
    public TagDoesNotExistException(String message) {
        super(message);
    }
}
