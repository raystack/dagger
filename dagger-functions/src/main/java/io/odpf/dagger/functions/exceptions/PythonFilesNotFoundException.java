package io.odpf.dagger.functions.exceptions;

/**
 * The type Python files not found exception.
 */
public class PythonFilesNotFoundException extends RuntimeException {

    /**
     * Instantiates a new Python files not found exception.
     *
     * @param message the message
     */
    public PythonFilesNotFoundException(String message) {
        super(message);
    }

}
