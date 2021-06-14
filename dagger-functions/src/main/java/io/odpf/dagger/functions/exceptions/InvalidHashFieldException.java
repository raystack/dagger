package io.odpf.dagger.functions.exceptions;

/**
 * The class Exception for Invalid hash field.
 */
public class InvalidHashFieldException extends RuntimeException {
    /**
     * Instantiates a new Invalid hash field exception.
     *
     * @param message the message
     */
    public InvalidHashFieldException(String message) {
        super(message);
    }

    /**
     * Instantiates a new Invalid hash field exception.
     *
     * @param innerException the inner exception
     */
    public InvalidHashFieldException(Exception innerException) {
        super(innerException);
    }
}
