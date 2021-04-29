package io.odpf.dagger.core.exception;

public class ThorCommandFailedException extends RuntimeException {
    public ThorCommandFailedException(String message) {
        super(message);
    }
}
