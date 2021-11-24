package io.odpf.dagger.core.metrics.reporters;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

import io.odpf.dagger.core.processors.telemetry.processor.MetricsTelemetryExporter;
import io.odpf.dagger.core.utils.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Error stats reporter.
 */
public class ErrorStatsReporter implements ErrorReporter {
    private MetricGroup metricGroup;
    private long shutDownPeriod;
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsTelemetryExporter.class.getName());

    public ErrorStatsReporter(MetricGroup metricGroup, long shutDownPeriod) {
        this.metricGroup = metricGroup;
        this.shutDownPeriod = shutDownPeriod;
    }

    @Override
    public void reportFatalException(Exception exception) {
        Counter counter = addExceptionToCounter(exception, metricGroup, Constants.FATAL_EXCEPTION_METRIC_GROUP_KEY);
        counter.inc();
        try {
            Thread.sleep(shutDownPeriod);
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage());
        }
    }

    @Override
    public void reportNonFatalException(Exception exception) {
        Counter counter = addExceptionToCounter(exception, metricGroup, Constants.NONFATAL_EXCEPTION_METRIC_GROUP_KEY);
        counter.inc();
    }
}
