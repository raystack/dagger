package com.gojek.daggers.metrics.reporters;

import com.gojek.daggers.utils.Constants;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static com.gojek.daggers.utils.Constants.TELEMETRY_ENABLED_KEY;
import static com.gojek.daggers.utils.Constants.TELEMETRY_ENABLED_VALUE_DEFAULT;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class ErrorReporterFactoryTest {

    @Mock
    Configuration configuration;

    @Mock
    RuntimeContext runtimeContext;

    @Before
    public void setup() {
        initMocks(this);
        when(configuration.getLong(Constants.SHUTDOWN_PERIOD_KEY, Constants.SHUTDOWN_PERIOD_DEFAULT)).thenReturn(0L);
        when(configuration.getBoolean(TELEMETRY_ENABLED_KEY, TELEMETRY_ENABLED_VALUE_DEFAULT)).thenReturn(true);
    }

    @Test
    public void shouldReturnErrorTelemetryFormConfigOnly() {
        ErrorReporter errorReporter = ErrorReporterFactory.getErrorReporter(runtimeContext, configuration);
        Assert.assertEquals(errorReporter.getClass(), ErrorStatsReporter.class);
    }

    @Test
    public void shouldReturnErrorStatsReporterIfTelemetryEnabled() {
        ErrorReporter errorReporter = ErrorReporterFactory.getErrorReporter(runtimeContext, true, 0L);
        Assert.assertEquals(errorReporter.getClass(), ErrorStatsReporter.class);
    }

    @Test
    public void shouldReturnNoOpReporterIfTelemetryDisabled() {
        ErrorReporter errorReporter = ErrorReporterFactory.getErrorReporter(runtimeContext, false, 0L);
        Assert.assertEquals(errorReporter.getClass(), NoOpErrorReporter.class);
    }

}