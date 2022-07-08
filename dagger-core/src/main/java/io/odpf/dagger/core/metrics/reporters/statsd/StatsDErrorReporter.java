package io.odpf.dagger.core.metrics.reporters.statsd;

import io.odpf.dagger.core.metrics.reporters.ErrorReporter;
import io.odpf.dagger.core.metrics.reporters.statsd.tags.StatsDTag;
import io.odpf.depot.metrics.StatsDReporter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

import static io.odpf.dagger.core.utils.Constants.FATAL_EXCEPTION_METRIC_GROUP_KEY;
import static io.odpf.dagger.core.utils.Constants.NONFATAL_EXCEPTION_METRIC_GROUP_KEY;

public class StatsDErrorReporter implements ErrorReporter {
    private static final String FATAL_EXCEPTION_TAG_KEY = "fatal_exception_type";
    private static final String NON_FATAL_EXCEPTION_TAG_KEY = "non_fatal_exception_type";
    private final StatsDReporter statsDReporter;

    public StatsDErrorReporter(SerializedStatsDReporterSupplier statsDReporterSupplier) {
        this.statsDReporter = statsDReporterSupplier.buildStatsDReporter();
    }

    @Override
    public void reportFatalException(Exception exception) {
        StatsDTag statsDTag = new StatsDTag(FATAL_EXCEPTION_TAG_KEY, exception.getClass().getName());
        statsDReporter.captureCount(FATAL_EXCEPTION_METRIC_GROUP_KEY, 1L, statsDTag.getFormattedTag());
    }

    @Override
    public void reportNonFatalException(Exception exception) {
        StatsDTag statsDTag = new StatsDTag(NON_FATAL_EXCEPTION_TAG_KEY, exception.getClass().getName());
        statsDReporter.captureCount(NONFATAL_EXCEPTION_METRIC_GROUP_KEY, 1L, statsDTag.getFormattedTag());
    }

    @Override
    public Counter addExceptionToCounter(Exception exception, MetricGroup metricGroup, String metricGroupKey) {
        throw new UnsupportedOperationException("This operation is not supported on StatsDErrorReporter");
    }
}
