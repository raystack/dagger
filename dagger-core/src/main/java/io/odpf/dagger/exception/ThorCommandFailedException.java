package io.odpf.dagger.exception;

public class ThorCommandFailedException extends RuntimeException {
    public ThorCommandFailedException(String message) {
        super(message);
    }
}
