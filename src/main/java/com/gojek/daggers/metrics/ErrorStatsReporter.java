package com.gojek.daggers.metrics;

import com.gojek.daggers.utils.Constants;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;

public class ErrorStatsReporter {

    private RuntimeContext runtimeContext;
    private long shutDownPeriod;

    public ErrorStatsReporter(RuntimeContext runtimeContext, Configuration configuration) {
        this.runtimeContext = runtimeContext;
        this.shutDownPeriod = getShutDownPeriod(configuration);
    }

    public ErrorStatsReporter(RuntimeContext runtimeContext, long shutDownPeriod) {
        this.runtimeContext = runtimeContext;
        this.shutDownPeriod = shutDownPeriod;
    }

    private long getShutDownPeriod(Configuration configuration) {
        return configuration.getLong(Constants.SHUTDOWN_PERIOD_KEY, Constants.SHUTDOWN_PERIOD_DEFAULT);
    }

    public void reportFatalException(Exception exception) {
        Counter counter = runtimeContext.getMetricGroup()
                .addGroup(Constants.FATAL_EXCEPTION_METRIC_GROUP_KEY, exception.getClass().getName()).counter("value");
        counter.inc();
        try {
            Thread.sleep(shutDownPeriod);
        } catch (InterruptedException e) {
            throw new RuntimeException("Error Sending Telemetry Metrics");
        }
    }

    public void reportNonFatalException(Exception exception) {
        Counter counter = runtimeContext.getMetricGroup()
                .addGroup(Constants.NONFATAL_EXCEPTION_METRIC_GROUP_KEY, exception.getClass().getName()).counter("value");
        counter.inc();
    }
}
