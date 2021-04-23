    package io.odpf.dagger.core.exception;

    public class DaggerDeserializationException extends RuntimeException {
        public DaggerDeserializationException(Exception innerException) {
            super(innerException);
        }
    }
