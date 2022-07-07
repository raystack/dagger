package io.odpf.dagger.core.metrics.reporters.statsd;

import io.odpf.dagger.core.metrics.reporters.ErrorReporter;
import io.odpf.dagger.core.metrics.reporters.statsd.tags.StatsDTag;
import io.odpf.depot.metrics.StatsDReporter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

import static io.odpf.dagger.core.utils.Constants.FATAL_EXCEPTION_METRIC_GROUP_KEY;
import static io.odpf.dagger.core.utils.Constants.NONFATAL_EXCEPTION_METRIC_GROUP_KEY;

public class StatsDErrorReporter implements ErrorReporter {
    private static final String fatalExceptionTag = new StatsDTag(FATAL_EXCEPTION_METRIC_GROUP_KEY).getFormattedTag();
    private static final String nonFatalExceptionTag = new StatsDTag(NONFATAL_EXCEPTION_METRIC_GROUP_KEY).getFormattedTag();
    private static final String FATAL_EXCEPTION_EVENT_MEASUREMENT = "fatal_exception_events";
    private static final String NON_FATAL_EXCEPTION_EVENT_MEASUREMENT = "non_fatal_exception_events";
    private final StatsDReporter statsDReporter;

    public StatsDErrorReporter(SerializedStatsDReporterSupplier statsDReporterSupplier) {
        this.statsDReporter = statsDReporterSupplier.getStatsDReporter();
    }

    @Override
    public void reportFatalException(Exception exception) {
        statsDReporter.captureCount(exception.getClass().getName(), 1L, fatalExceptionTag);
        statsDReporter.recordEvent(FATAL_EXCEPTION_EVENT_MEASUREMENT, exception.getMessage(), fatalExceptionTag);
    }

    @Override
    public void reportNonFatalException(Exception exception) {
        statsDReporter.captureCount(exception.getClass().getName(), 1L, nonFatalExceptionTag);
        statsDReporter.recordEvent(NON_FATAL_EXCEPTION_EVENT_MEASUREMENT, exception.getMessage(), nonFatalExceptionTag);
    }

    @Override
    public Counter addExceptionToCounter(Exception exception, MetricGroup metricGroup, String metricGroupKey) {
        throw new UnsupportedOperationException("This operation is not supported on StatsDReporter");
    }
}
