package com.gotocompany.dagger.core.exception;

import java.io.IOException;

public class InfluxWriteException extends IOException {
    public InfluxWriteException(Throwable err) {
        super(err);
    }
}
