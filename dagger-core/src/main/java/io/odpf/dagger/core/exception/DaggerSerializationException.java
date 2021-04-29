package io.odpf.dagger.core.exception;

public class DaggerSerializationException extends RuntimeException {
    public DaggerSerializationException(String protoClassMisconfiguredError) {
        super(protoClassMisconfiguredError);
    }
}
