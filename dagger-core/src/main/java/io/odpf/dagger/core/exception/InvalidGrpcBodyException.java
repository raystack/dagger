package io.odpf.dagger.core.exception;

public class InvalidGrpcBodyException extends RuntimeException  {

    public InvalidGrpcBodyException(String message) {
        super(message);
    }
}
