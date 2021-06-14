package io.odpf.dagger.core.exception;

/**
 * The class Exception if error happens on input output mapping.
 */
public class InputOutputMappingException extends RuntimeException {
    /**
     * Instantiates a new Input output mapping exception.
     *
     * @param message the message
     */
    public InputOutputMappingException(String message) {
        super(message);
    }
}
