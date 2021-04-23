package io.odpf.dagger.core.metrics.reporters;

public class NoOpErrorReporter implements ErrorReporter {
    @Override
    public void reportFatalException(Exception exception) {

    }

    @Override
    public void reportNonFatalException(Exception exception) {

    }
}
