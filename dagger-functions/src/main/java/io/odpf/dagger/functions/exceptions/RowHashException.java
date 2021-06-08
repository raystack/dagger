package io.odpf.dagger.functions.exceptions;

public class RowHashException extends RuntimeException {
    public RowHashException(String message) {
        super(message);
    }

    public RowHashException(Exception innerException) {
        super(innerException);
    }

    public RowHashException(String message, Exception innerException) {
        super(message, innerException);
    }
}
