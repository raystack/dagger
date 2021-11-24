package io.odpf.dagger.core.metrics.reporters;

import org.apache.flink.metrics.MetricGroup;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.core.utils.Constants;

/**
 * The Factory class for Error reporter.
 */
public class ErrorReporterFactory {

    /**
     * Gets error reporter.
     *
     * @param metricGroup   the runtime context
     * @param configuration the configuration
     * @return the error reporter
     */
    public static ErrorReporter getErrorReporter(MetricGroup metricGroup, Configuration configuration) {
        long shutDownPeriod = configuration.getLong(Constants.METRIC_TELEMETRY_SHUTDOWN_PERIOD_MS_KEY, Constants.METRIC_TELEMETRY_SHUTDOWN_PERIOD_MS_DEFAULT);
        boolean telemetryEnabled = configuration.getBoolean(Constants.METRIC_TELEMETRY_ENABLE_KEY, Constants.METRIC_TELEMETRY_ENABLE_VALUE_DEFAULT);
        return getErrorReporter(metricGroup, telemetryEnabled, shutDownPeriod);
    }

    /**
     * Gets error reporter.
     *
     * @param metricGroup     the metric-group
     * @param telemetryEnable the telemetry enable
     * @param shutDownPeriod  the shut down period
     * @return the error reporter
     */
    public static ErrorReporter getErrorReporter(MetricGroup metricGroup, Boolean telemetryEnable, long shutDownPeriod) {
        if (telemetryEnable) {
            return new ErrorStatsReporter(metricGroup, shutDownPeriod);
        } else {
            return new NoOpErrorReporter();
        }
    }
}
