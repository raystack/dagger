package com.gojek.daggers.metrics;

import com.gojek.daggers.utils.Constants;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;

public class ErrorStatsReporter {

    private RuntimeContext runtimeContext;

    public ErrorStatsReporter(RuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;
    }

    public void reportFatalException(Exception exception) throws InterruptedException {
        Counter counter = runtimeContext.getMetricGroup()
                .addGroup(Constants.FATAL_EXCEPTION_METRIC_GROUP_KEY, exception.getClass().getName()).counter("value");
        counter.inc();
        Thread.sleep(Constants.THREAD_SLEEP_DURATION_MILLIS);
    }
}
