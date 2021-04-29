package io.odpf.dagger.core.metrics.reporters;

public interface ErrorReporter {
    void reportFatalException(Exception exception);

    void reportNonFatalException(Exception exception);
}
