package io.odpf.dagger.core.exception;

/**
 * The class Exception if Transformer class is not defined in transformer configuration.
 */
public class TransformClassNotDefinedException extends RuntimeException {
    /**
     * Instantiates a new Transform class not defined exception.
     *
     * @param message the message
     */
    public TransformClassNotDefinedException(String message) {
        super(message);
    }
}
