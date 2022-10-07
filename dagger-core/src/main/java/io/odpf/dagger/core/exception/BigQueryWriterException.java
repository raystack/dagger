package io.odpf.dagger.core.exception;

import java.io.IOException;

public class BigQueryWriterException extends IOException {

    public BigQueryWriterException(String message, Throwable cause) {
        super(message, cause);
    }

    public BigQueryWriterException(String message) {
        super(message);
    }
}
