package io.odpf.dagger.core.exception;

/**
 * The class Exception if there is failure on Thor command.
 */
public class ThorCommandFailedException extends RuntimeException {
    /**
     * Instantiates a new Thor command failed exception.
     *
     * @param message the message
     */
    public ThorCommandFailedException(String message) {
        super(message);
    }
}
