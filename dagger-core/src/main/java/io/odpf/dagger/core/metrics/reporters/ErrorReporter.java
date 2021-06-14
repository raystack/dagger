package io.odpf.dagger.core.metrics.reporters;

/**
 * The interface Error reporter.
 */
public interface ErrorReporter {
    /**
     * Report fatal exception.
     *
     * @param exception the exception
     */
    void reportFatalException(Exception exception);

    /**
     * Report non fatal exception.
     *
     * @param exception the exception
     */
    void reportNonFatalException(Exception exception);
}
