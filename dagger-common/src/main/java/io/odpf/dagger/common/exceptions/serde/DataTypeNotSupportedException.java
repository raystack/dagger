package io.odpf.dagger.common.exceptions.serde;

/**
 * The class Exception for unsupported protobuf data type.
 */
public class DataTypeNotSupportedException extends RuntimeException {
    /**
     * Instantiates a new Data type not supported exception.
     *
     * @param message the message
     */
    public DataTypeNotSupportedException(String message) {
        super(message);
    }
}
