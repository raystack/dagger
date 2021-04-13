package io.odpf.dagger.metrics.reporters;

public class NoOpErrorReporter implements ErrorReporter {
    @Override
    public void reportFatalException(Exception exception) {

    }

    @Override
    public void reportNonFatalException(Exception exception) {

    }
}
