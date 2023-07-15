package org.raystack.dagger.common.exceptions.serde;

public class InvalidJSONSchemaException extends RuntimeException {
    public InvalidJSONSchemaException(Exception innerException) {
        super(innerException);
    }
}
