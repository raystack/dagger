package io.odpf.dagger.common.exceptions.serde;

/**
 * The class Exception if there is an Invalid Data type.
 */
public class InvalidDataTypeException extends RuntimeException {
    /**
     * Instantiates a new Invalid data type exception.
     *
     * @param message the message
     */
    public InvalidDataTypeException(String message) {
        super(message);
    }
}
