package io.odpf.dagger.functions.exceptions;

/**
 * The class Exception if Median not found on OutlierMad Udf.
 */
public class MedianNotFound extends RuntimeException {
    /**
     * Instantiates a new Median not found.
     *
     * @param message the message
     */
    public MedianNotFound(String message) {
        super(message);
    }
}
