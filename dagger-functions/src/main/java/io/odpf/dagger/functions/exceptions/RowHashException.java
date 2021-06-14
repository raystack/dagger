package io.odpf.dagger.functions.exceptions;

/**
 * The class Exception if failed on hashing the Row.
 */
public class RowHashException extends RuntimeException {
    /**
     * Instantiates a new Row hash exception.
     *
     * @param message the message
     */
    public RowHashException(String message) {
        super(message);
    }

    /**
     * Instantiates a new Row hash exception.
     *
     * @param innerException the inner exception
     */
    public RowHashException(Exception innerException) {
        super(innerException);
    }

    /**
     * Instantiates a new Row hash exception.
     *
     * @param message        the message
     * @param innerException the inner exception
     */
    public RowHashException(String message, Exception innerException) {
        super(message, innerException);
    }
}
