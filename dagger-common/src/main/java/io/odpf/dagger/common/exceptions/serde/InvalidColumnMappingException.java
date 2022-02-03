package io.odpf.dagger.common.exceptions.serde;

/**
 * The class Exception if there is Invalid Column Mapping.
 */
public class InvalidColumnMappingException extends RuntimeException {
    /**
     * Instantiates a new Invalid column mapping exception with specified message.
     *
     * @param message the message
     */
    public InvalidColumnMappingException(String message) {
        super(message);
    }

    /**
     * Instantiates a new Invalid column mapping exception with specified message and error message.
     *
     * @param message the message
     * @param err     the err
     */
    public InvalidColumnMappingException(String message, Throwable err) {
        super(message, err);
    }
}
