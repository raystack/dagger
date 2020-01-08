    package com.gojek.daggers.exception;

    public class DaggerDeserializationException extends RuntimeException {
        public static final String DAGGER_DESERIALIZATION_EXCEPTION = "dagger deserialization exception";

        public DaggerDeserializationException() {
            this(DAGGER_DESERIALIZATION_EXCEPTION);
        }

        public DaggerDeserializationException(String protoClassMisconfiguredError) {
            super(protoClassMisconfiguredError);
        }

        public DaggerDeserializationException(Exception innerException) {
            super(innerException);
        }
    }
