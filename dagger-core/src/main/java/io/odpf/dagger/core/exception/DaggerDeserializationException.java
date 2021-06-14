package io.odpf.dagger.core.exception;

/**
 * The class Exception if failed on Deserialize the protobuf message.
 */
public class DaggerDeserializationException extends RuntimeException {
    /**
     * Instantiates a new Dagger deserialization exception.
     *
     * @param innerException the inner exception
     */
    public DaggerDeserializationException(Exception innerException) {
        super(innerException);
    }
}
