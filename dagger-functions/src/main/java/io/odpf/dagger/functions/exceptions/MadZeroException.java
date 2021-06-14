package io.odpf.dagger.functions.exceptions;

/**
 * The class Exception if Mad is zero on OutlierMad Udf.
 */
public class MadZeroException extends RuntimeException {
    /**
     * Instantiates a new Mad zero exception.
     *
     * @param message the message
     */
    public MadZeroException(String message) {
        super(message);
    }
}
