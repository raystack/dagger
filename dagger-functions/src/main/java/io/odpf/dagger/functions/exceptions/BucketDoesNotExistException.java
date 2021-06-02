package io.odpf.dagger.functions.exceptions;

public class BucketDoesNotExistException extends RuntimeException {
    public BucketDoesNotExistException(String message) {
        super(message);
    }
}
