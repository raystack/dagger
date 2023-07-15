package org.raystack.dagger.common.exceptions.serde;

/**
 * This runtime exception is thrown when a field cannot be parsed from a Parquet SimpleGroup.
 **/
public class SimpleGroupParsingException extends RuntimeException {

    public SimpleGroupParsingException(String message) {
        super(message);
    }
}
