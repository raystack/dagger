package io.odpf.dagger.core.exception;

/**
 * The class Exception if there is records that not consumed.
 */
public class RecordsNotConsumedException extends RuntimeException {
    /**
     * Instantiates a new Records not consumed exception.
     *
     * @param message the message
     */
    public RecordsNotConsumedException(String message) {
        super(message);
    }
}
