package io.odpf.dagger.core.metrics.reporters;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class MetricGroupErrorReporterTest {
    @Mock
    private MetricGroup metricGroup;

    @Mock
    private Counter counter;

    private MetricGroupErrorReporter metricGroupErrorReporter;

    @Before
    public void setup() {
        initMocks(this);
        long shutDownPeriod = 0L;
        metricGroupErrorReporter = new MetricGroupErrorReporter(metricGroup, shutDownPeriod);
    }

    @Test
    public void shouldReportError() {
        when(metricGroup.addGroup("fatal.exception", "java.lang.RuntimeException")).thenReturn(metricGroup);
        when(metricGroup.counter("value")).thenReturn(counter);

        metricGroupErrorReporter.reportFatalException(new RuntimeException());
        verify(counter, times(1)).inc();
    }

    @Test
    public void shouldReportNonFatalError() {
        when(metricGroup.addGroup("non.fatal.exception", "java.lang.RuntimeException")).thenReturn(metricGroup);
        when(metricGroup.counter("value")).thenReturn(counter);
        metricGroupErrorReporter.reportNonFatalException(new RuntimeException());

        verify(counter, times(1)).inc();
    }
}
