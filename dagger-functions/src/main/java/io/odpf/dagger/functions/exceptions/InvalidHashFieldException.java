package io.odpf.dagger.functions.exceptions;

public class InvalidHashFieldException extends RuntimeException {
    public InvalidHashFieldException(String message) {
        super(message);
    }

    public InvalidHashFieldException(Exception innerException) {
        super(innerException);
    }
}
