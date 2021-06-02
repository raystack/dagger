package io.odpf.dagger.functions.exceptions;

public class KeyDoesNotExistException extends RuntimeException {
    public KeyDoesNotExistException(String message) {
        super(message);
    }
}
