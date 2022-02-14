package io.odpf.dagger.core.exception;

/**
 * The class Exception if there is an invalid Function internal postprocessor.
 */
public class InvalidFunctionException extends RuntimeException {
    /**
     * Instantiates a new Invalid function exception.
     *
     * @param message the message
     */
    public InvalidFunctionException(String message) {
        super(message);
    }
}
