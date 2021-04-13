package io.odpf.dagger.exception;

public class InvalidGrpcBodyException extends RuntimeException  {

    public InvalidGrpcBodyException(String message) {
        super(message);
    }
}
