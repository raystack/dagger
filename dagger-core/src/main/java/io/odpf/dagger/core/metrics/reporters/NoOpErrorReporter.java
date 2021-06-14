package io.odpf.dagger.core.metrics.reporters;

/**
 * The No op error reporter.
 */
public class NoOpErrorReporter implements ErrorReporter {
    @Override
    public void reportFatalException(Exception exception) {

    }

    @Override
    public void reportNonFatalException(Exception exception) {

    }
}
