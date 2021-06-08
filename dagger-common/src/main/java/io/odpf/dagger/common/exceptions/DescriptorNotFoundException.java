package io.odpf.dagger.common.exceptions;

/**
 * The class Exception if Descriptor not found.
 */
public class DescriptorNotFoundException extends RuntimeException {
    public static final String DESCRIPTOR_NOT_FOUND = "descriptor not found";

    /**
     * Instantiates a new Descriptor not found exception.
     */
    public DescriptorNotFoundException() {
        this(DESCRIPTOR_NOT_FOUND);
    }

    /**
     * Instantiates a new Descriptor not found exception with the specified detail message.
     *
     * @param protoClassMisconfiguredError the proto class misconfigured error
     */
    public DescriptorNotFoundException(String protoClassMisconfiguredError) {
        super(protoClassMisconfiguredError);
    }

    /**
     * Instantiates a new Descriptor not found exception with an inner exception as detail message.
     *
     * @param innerException the inner exception
     */
    public DescriptorNotFoundException(Exception innerException) {
        super(innerException);
    }
}
