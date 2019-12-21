package com.gojek.daggers.metrics;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class ErrorStatsReporterTest {

    @Mock
    private RuntimeContext runtimeContext;

    @Mock
    private MetricGroup metricGroup;

    @Mock
    private Counter counter;

    private ErrorStatsReporter errorStatsReporter;

    @Before
    public void setup() {
        initMocks(this);
        errorStatsReporter = new ErrorStatsReporter(runtimeContext);
    }

    @Test
    public void shouldReportError() throws InterruptedException {
        when(runtimeContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup("fatal.exception", "java.lang.RuntimeException")).thenReturn(metricGroup);
        when(metricGroup.counter("value")).thenReturn(counter);
        errorStatsReporter.reportFatalException(new RuntimeException());

        verify(counter, times(1)).inc();
    }
}