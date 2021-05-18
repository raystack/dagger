package io.odpf.dagger.core.metrics.reporters;

import io.odpf.dagger.core.utils.Constants;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

public class ErrorReporterFactory {

    public static ErrorReporter getErrorReporter(RuntimeContext runtimeContext, Configuration configuration) {
        long shutDownPeriod = configuration.getLong(Constants.METRIC_TELEMETRY_SHUTDOWN_PERIOD_MS_KEY, Constants.METRIC_TELEMETRY_SHUTDOWN_PERIOD_MS_DEFAULT);
        boolean telemetryEnabled = configuration.getBoolean(Constants.METRIC_TELEMETRY_ENABLE_KEY, Constants.METRIC_TELEMETRY_ENABLE_VALUE_DEFAULT);
        return getErrorReporter(runtimeContext, telemetryEnabled, shutDownPeriod);
    }

    public static ErrorReporter getErrorReporter(RuntimeContext runtimeContext, Boolean telemetryEnable, long shutDownPeriod) {
        if (telemetryEnable) {
            return new ErrorStatsReporter(runtimeContext, shutDownPeriod);
        } else {
            return new NoOpErrorReporter();
        }
    }
}
