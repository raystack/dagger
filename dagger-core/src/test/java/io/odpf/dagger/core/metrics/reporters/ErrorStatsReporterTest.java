package io.odpf.dagger.core.metrics.reporters;

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

    private long shutDownPeriod;

    @Before
    public void setup() {
        initMocks(this);
        shutDownPeriod = 0L;
        errorStatsReporter = new ErrorStatsReporter(runtimeContext, shutDownPeriod);
    }

    @Test
    public void shouldReportError() {
        when(runtimeContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup("fatal.exception", "java.lang.RuntimeException")).thenReturn(metricGroup);
        when(metricGroup.counter("value")).thenReturn(counter);
        errorStatsReporter.reportFatalException(new RuntimeException());

        verify(counter, times(1)).inc();
    }

    @Test
    public void shouldReportNonFatalError() {
        when(runtimeContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup("non.fatal.exception", "java.lang.RuntimeException")).thenReturn(metricGroup);
        when(metricGroup.counter("value")).thenReturn(counter);
        errorStatsReporter.reportNonFatalException(new RuntimeException());

        verify(counter, times(1)).inc();
    }
}
