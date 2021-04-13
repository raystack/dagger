    package io.odpf.dagger.exception;

    public class DaggerDeserializationException extends RuntimeException {
        public DaggerDeserializationException(Exception innerException) {
            super(innerException);
        }
    }
