package io.odpf.dagger.core.exception;

/**
 * The class Exception if Grpc Channel not available.
 */
public class ChannelNotAvailableException extends Exception {

    /**
     * Instantiates a new Channel not available exception.
     *
     * @param message the message
     */
    public ChannelNotAvailableException(String message) {
        super(message);
    }

}
