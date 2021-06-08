package io.odpf.dagger.functions.exceptions;

public class OddNumberOfArgumentsException extends RuntimeException {

    private static final String DEFAULT_ERROR_MESSAGE = "Odd number of arguments given to Udf. Requires even.";

    public OddNumberOfArgumentsException() {
        super(DEFAULT_ERROR_MESSAGE);
    }

    public OddNumberOfArgumentsException(String message) {
        super(message);
    }
}
