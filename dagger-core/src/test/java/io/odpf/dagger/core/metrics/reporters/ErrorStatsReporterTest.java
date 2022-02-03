package io.odpf.dagger.core.metrics.reporters;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.OperatorMetricGroup;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class ErrorStatsReporterTest {

    @Mock
    private RuntimeContext runtimeContext;

    @Mock
    private OperatorMetricGroup metricGroup;

    @Mock
    private Counter counter;

    private ErrorStatsReporter errorStatsReporter;

    @Before
    public void setup() {
        initMocks(this);
        long shutDownPeriod = 0L;
        when(runtimeContext.getMetricGroup()).thenReturn(metricGroup);
        errorStatsReporter = new ErrorStatsReporter(runtimeContext.getMetricGroup(), shutDownPeriod);
    }

    @Test
    public void shouldReportError() {
        when(metricGroup.addGroup("fatal.exception", "java.lang.RuntimeException")).thenReturn(metricGroup);
        when(metricGroup.counter("value")).thenReturn(counter);
        errorStatsReporter.reportFatalException(new RuntimeException());

        verify(counter, times(1)).inc();
    }

    @Test
    public void shouldReportNonFatalError() {
        when(metricGroup.addGroup("non.fatal.exception", "java.lang.RuntimeException")).thenReturn(metricGroup);
        when(metricGroup.counter("value")).thenReturn(counter);
        errorStatsReporter.reportNonFatalException(new RuntimeException());

        verify(counter, times(1)).inc();
    }
}
