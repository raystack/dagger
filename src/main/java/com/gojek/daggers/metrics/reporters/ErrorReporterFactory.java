package com.gojek.daggers.metrics.reporters;

import com.gojek.daggers.utils.Constants;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

import static com.gojek.daggers.utils.Constants.TELEMETRY_ENABLED_KEY;
import static com.gojek.daggers.utils.Constants.TELEMETRY_ENABLED_VALUE_DEFAULT;

public class ErrorReporterFactory {

    public static ErrorReporter getErrorReporter(RuntimeContext runtimeContext, Configuration configuration) {
        long shutDownPeriod = configuration.getLong(Constants.SHUTDOWN_PERIOD_KEY, Constants.SHUTDOWN_PERIOD_DEFAULT);
        boolean telemetryEnabled = configuration.getBoolean(TELEMETRY_ENABLED_KEY, TELEMETRY_ENABLED_VALUE_DEFAULT);
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
