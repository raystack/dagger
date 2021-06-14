package io.odpf.dagger.core.exception;

/**
 * The class Exception if there is an Invalid Configuration.
 */
public class InvalidConfigurationException extends RuntimeException {
    /**
     * Instantiates a new Invalid configuration exception.
     *
     * @param message the message
     */
    public InvalidConfigurationException(String message) {
        super(message);
    }
}
