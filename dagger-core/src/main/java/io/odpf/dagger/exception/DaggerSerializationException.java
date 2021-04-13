package io.odpf.dagger.exception;

public class DaggerSerializationException extends RuntimeException {
    public DaggerSerializationException(String protoClassMisconfiguredError) {
        super(protoClassMisconfiguredError);
    }
}
