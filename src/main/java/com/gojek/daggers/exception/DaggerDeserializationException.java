    package com.gojek.daggers.exception;

    public class DaggerDeserializationException extends RuntimeException {
        public DaggerDeserializationException(Exception innerException) {
            super(innerException);
        }
    }
