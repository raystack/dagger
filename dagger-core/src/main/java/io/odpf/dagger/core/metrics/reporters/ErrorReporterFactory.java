package io.odpf.dagger.core.metrics.reporters;

import io.odpf.dagger.core.utils.Constants;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

public class ErrorReporterFactory {

    public static ErrorReporter getErrorReporter(RuntimeContext runtimeContext, Configuration configuration) {
        long shutDownPeriod = configuration.getLong(Constants.SHUTDOWN_PERIOD_KEY, Constants.SHUTDOWN_PERIOD_DEFAULT);
        boolean telemetryEnabled = configuration.getBoolean(Constants.TELEMETRY_ENABLED_KEY, Constants.TELEMETRY_ENABLED_VALUE_DEFAULT);
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
