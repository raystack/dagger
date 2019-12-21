package com.gojek.daggers.exception;

public class DescriptorNotFoundException extends RuntimeException {
    public static final String DESCRIPTOR_NOT_FOUND = "descriptor not found";

    public DescriptorNotFoundException() {
        this(DESCRIPTOR_NOT_FOUND);
    }

    public DescriptorNotFoundException(String protoClassMisconfiguredError) {
        super(protoClassMisconfiguredError);
    }

    public DescriptorNotFoundException(Exception innerException) {
        super(innerException);
    }
}
