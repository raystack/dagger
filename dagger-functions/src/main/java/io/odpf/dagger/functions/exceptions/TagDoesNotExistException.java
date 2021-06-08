package io.odpf.dagger.functions.exceptions;

public class TagDoesNotExistException extends RuntimeException {
    public TagDoesNotExistException(String message) {
        super(message);
    }
}
