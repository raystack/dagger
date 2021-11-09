package io.odpf.dagger.core.exception;

import java.io.IOException;

public class InfluxWriteException extends IOException {
    public InfluxWriteException(Throwable err) {
        super(err);
    }
}
