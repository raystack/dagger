package com.gojek.daggers.metrics.reporters;

import com.gojek.daggers.postProcessors.telemetry.processor.MetricsTelemetryExporter;
import com.gojek.daggers.utils.Constants;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErrorStatsReporter implements ErrorReporter {
    private RuntimeContext runtimeContext;
    private long shutDownPeriod;
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsTelemetryExporter.class.getName());


    public ErrorStatsReporter(RuntimeContext runtimeContext, long shutDownPeriod) {
        this.runtimeContext = runtimeContext;
        this.shutDownPeriod = shutDownPeriod;
    }

    @Override
    public void reportFatalException(Exception exception) {
        Counter counter = runtimeContext.getMetricGroup()
                .addGroup(Constants.FATAL_EXCEPTION_METRIC_GROUP_KEY, exception.getClass().getName()).counter("value");
        counter.inc();
        try {
            Thread.sleep(shutDownPeriod);
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage());
        }
    }

    @Override
    public void reportNonFatalException(Exception exception) {
        Counter counter = runtimeContext.getMetricGroup()
                .addGroup(Constants.NONFATAL_EXCEPTION_METRIC_GROUP_KEY, exception.getClass().getName()).counter("value");
        counter.inc();
    }
}
