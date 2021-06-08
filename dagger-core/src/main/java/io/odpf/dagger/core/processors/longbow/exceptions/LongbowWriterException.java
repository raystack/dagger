package io.odpf.dagger.core.processors.longbow.exceptions;

/**
 * The Exception for Longbow writer.
 */
public class LongbowWriterException extends RuntimeException {
    /**
     * Instantiates a new Longbow writer exception.
     *
     * @param throwable the throwable
     */
    public LongbowWriterException(Throwable throwable) {
        super(throwable);
    }
}
