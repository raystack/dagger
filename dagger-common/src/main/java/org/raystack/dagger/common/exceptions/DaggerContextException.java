package org.raystack.dagger.common.exceptions;

/**
 * The class Exception if there is something wrong with Dagger context object.
 */
public class DaggerContextException extends RuntimeException {

    /**
     * Instantiates a new Dagger context exception with specified error message.
     *
     * @param message the message
     */
    public DaggerContextException(String message) {
        super(message);
    }
}
