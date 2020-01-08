package com.gojek.daggers.exception;

public class DaggerSerializationException extends RuntimeException {
    public static final String DAGGER_SERIALIZATION_EXCEPTION = "dagger serialization exception";

    public DaggerSerializationException() {
        this(DAGGER_SERIALIZATION_EXCEPTION);
    }

    public DaggerSerializationException(String protoClassMisconfiguredError) {
        super(protoClassMisconfiguredError);
    }
}
