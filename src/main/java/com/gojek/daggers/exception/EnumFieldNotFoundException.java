package com.gojek.daggers.exception;

public class EnumFieldNotFoundException extends RuntimeException {
    public EnumFieldNotFoundException(String message) {
        super(message);
    }
}
