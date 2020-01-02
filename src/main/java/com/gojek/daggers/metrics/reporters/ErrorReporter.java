package com.gojek.daggers.metrics.reporters;

public interface ErrorReporter {
    void reportFatalException(Exception exception);

    void reportNonFatalException(Exception exception);
}
