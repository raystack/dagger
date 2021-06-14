package io.odpf.dagger.core.exception;

/**
 * The class Exception if Udf factory class is not defined in function factory configuration.
 */
public class UDFFactoryClassNotDefinedException extends RuntimeException {
    /**
     * Instantiates a new Udf factory class not defined exception.
     *
     * @param message the message
     */
    public UDFFactoryClassNotDefinedException(String message) {
        super(message);
    }
}
