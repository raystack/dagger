package io.odpf.dagger.functions.exceptions;

public class InvalidNumberOfArgumentsException extends RuntimeException {
    private static final String DEFAULT_ERROR_MESSAGE = "Invalid number of arguments given to Udf. Requires arguments that is divisible by 3.";

    public InvalidNumberOfArgumentsException() {
        super(DEFAULT_ERROR_MESSAGE);
    }

    public InvalidNumberOfArgumentsException(String message) {
        super(message);
    }
}
