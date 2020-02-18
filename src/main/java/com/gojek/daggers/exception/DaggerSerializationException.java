package com.gojek.daggers.exception;

public class DaggerSerializationException extends RuntimeException {
    public DaggerSerializationException(String protoClassMisconfiguredError) {
        super(protoClassMisconfiguredError);
    }
}
