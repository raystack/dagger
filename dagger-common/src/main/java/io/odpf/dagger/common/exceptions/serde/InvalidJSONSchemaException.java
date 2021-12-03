package io.odpf.dagger.common.exceptions.serde;

public class InvalidJSONSchemaException extends RuntimeException {
    public InvalidJSONSchemaException(Exception innerException) {
        super(innerException);
    }
}
