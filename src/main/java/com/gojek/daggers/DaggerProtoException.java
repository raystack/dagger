package com.gojek.daggers;

public class DaggerProtoException extends RuntimeException {
    public static final String PROTO_CLASS_MISCONFIGURED_ERROR = "proto class misconfigured";

    public DaggerProtoException() {
        this(PROTO_CLASS_MISCONFIGURED_ERROR);
    }

    public DaggerProtoException(String message) {
        super(message);
    }

    public DaggerProtoException(String message, Exception innerException) {
        super(message, innerException);
    }

    public DaggerProtoException(Exception innerException) {
        super(innerException);
    }
}
