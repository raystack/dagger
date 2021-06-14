package io.odpf.dagger.core.exception;

/**
 * The class Exception if there is something wrong with Dagger configuration.
 */
public class DaggerConfigurationException extends RuntimeException {

    /**
     * Instantiates a new Dagger configuration exception with specified error message.
     *
     * @param message the message
     */
    public DaggerConfigurationException(String message) {
        super(message);
    }
}
