package org.raystack.dagger.core.metrics.reporters;

import org.raystack.dagger.core.utils.Constants;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.MetricGroup;

import org.raystack.dagger.common.configuration.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class ErrorReporterFactoryTest {

    @Mock
    private RuntimeContext runtimeContext;

    @Mock
    private Configuration configuration;

    @Mock
    private MetricGroup metricGroup;

    @Before
    public void setup() {
        initMocks(this);
        when(configuration.getLong(Constants.METRIC_TELEMETRY_SHUTDOWN_PERIOD_MS_KEY, Constants.METRIC_TELEMETRY_SHUTDOWN_PERIOD_MS_DEFAULT)).thenReturn(0L);
        when(configuration.getBoolean(Constants.METRIC_TELEMETRY_ENABLE_KEY, Constants.METRIC_TELEMETRY_ENABLE_VALUE_DEFAULT)).thenReturn(true);
    }

    @Test
    public void shouldReturnErrorTelemetryFormConfigOnly() {
        ErrorReporter errorReporter = ErrorReporterFactory.getErrorReporter(runtimeContext.getMetricGroup(), configuration);
        assertEquals(errorReporter.getClass(), ErrorStatsReporter.class);
    }

    @Test
    public void shouldReturnErrorTelemetryFormMetricGroup() {
        ErrorReporter errorReporter = ErrorReporterFactory.getErrorReporter(metricGroup, configuration);
        assertEquals(errorReporter.getClass(), ErrorStatsReporter.class);
    }

    @Test
    public void shouldReturnErrorStatsReporterIfTelemetryEnabled() {
        ErrorReporter errorReporter = ErrorReporterFactory.getErrorReporter(runtimeContext.getMetricGroup(), true, 0L);
        assertEquals(errorReporter.getClass(), ErrorStatsReporter.class);
    }

    @Test
    public void shouldReturnNoOpReporterIfTelemetryDisabled() {
        ErrorReporter errorReporter = ErrorReporterFactory.getErrorReporter(runtimeContext.getMetricGroup(), false, 0L);
        assertEquals(errorReporter.getClass(), NoOpErrorReporter.class);
    }
}
