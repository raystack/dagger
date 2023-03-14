package com.gotocompany.dagger.core.metrics.reporters.statsd;

import com.gotocompany.depot.metrics.StatsDReporter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

public class StatsDErrorReporterTest {
    @Mock
    private StatsDReporter statsDReporter;

    @Before
    public void setup() {
        initMocks(this);
    }

    private final SerializedStatsDReporterSupplier statsDReporterSupplier = () -> statsDReporter;

    @Test
    public void shouldReportFatalExceptionWithExceptionClassNameAsTagValue() {
        StatsDErrorReporter statsDErrorReporter = new StatsDErrorReporter(statsDReporterSupplier);

        statsDErrorReporter.reportFatalException(new NullPointerException("This is a fatal exception"));

        verify(statsDReporter, times(1))
                .captureCount("fatal.exception", 1L,
                        "fatal_exception_type=" + NullPointerException.class.getName());
    }

    @Test
    public void shouldReportNonFatalExceptionWithExceptionClassNameAsTagValue() {
        StatsDErrorReporter statsDErrorReporter = new StatsDErrorReporter(statsDReporterSupplier);

        statsDErrorReporter.reportNonFatalException(new Exception("This is a non-fatal exception"));

        verify(statsDReporter, times(1))
                .captureCount("non.fatal.exception", 1L,
                        "non_fatal_exception_type=" + Exception.class.getName());
    }

    @Test
    public void shouldThrowUnsupportedExceptionForAddExceptionToCounter() {
        StatsDErrorReporter statsDErrorReporter = new StatsDErrorReporter(statsDReporterSupplier);

        UnsupportedOperationException ex = assertThrows(UnsupportedOperationException.class,
                () -> statsDErrorReporter.addExceptionToCounter(null, null, null));

        assertEquals("This operation is not supported on StatsDErrorReporter", ex.getMessage());
    }
}
