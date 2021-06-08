package io.odpf.dagger.functions.exceptions;

/**
 * The class Exception if Bucket does not exist.
 */
public class BucketDoesNotExistException extends RuntimeException {
    /**
     * Instantiates a new Bucket does not exist exception.
     *
     * @param message the message
     */
    public BucketDoesNotExistException(String message) {
        super(message);
    }
}
