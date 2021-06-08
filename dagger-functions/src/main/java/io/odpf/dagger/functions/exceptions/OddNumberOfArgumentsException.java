package io.odpf.dagger.functions.exceptions;

/**
 * The class Exception for Odd number of arguments on Features Udf.
 */
public class OddNumberOfArgumentsException extends RuntimeException {

    private static final String DEFAULT_ERROR_MESSAGE = "Odd number of arguments given to Udf. Requires even.";

    /**
     * Instantiates a new Odd number of arguments exception.
     */
    public OddNumberOfArgumentsException() {
        super(DEFAULT_ERROR_MESSAGE);
    }

    /**
     * Instantiates a new Odd number of arguments exception.
     *
     * @param message the message
     */
    public OddNumberOfArgumentsException(String message) {
        super(message);
    }
}
