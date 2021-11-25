package io.odpf.dagger.core.metrics.reporters;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

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


    default Counter addExceptionToCounter(Exception exception, MetricGroup metricGroup, String metricGroupKey) {
        return metricGroup.addGroup(metricGroupKey, exception.getClass().getName()).counter("value");
    }
}
