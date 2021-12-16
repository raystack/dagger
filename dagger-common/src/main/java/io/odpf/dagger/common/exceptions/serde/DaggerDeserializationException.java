package io.odpf.dagger.common.exceptions.serde;

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

    public DaggerDeserializationException(String message) {
        super(message);
    }
}
