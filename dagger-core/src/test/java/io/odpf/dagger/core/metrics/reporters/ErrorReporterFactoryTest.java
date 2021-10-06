package io.odpf.dagger.core.metrics.reporters;

import io.odpf.dagger.common.configuration.UserConfiguration;
import io.odpf.dagger.core.utils.Constants;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.utils.ParameterTool;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static io.odpf.dagger.core.utils.Constants.METRIC_TELEMETRY_ENABLE_KEY;
import static io.odpf.dagger.core.utils.Constants.METRIC_TELEMETRY_ENABLE_VALUE_DEFAULT;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class ErrorReporterFactoryTest {

    @Mock
    private ParameterTool parameterTool;

    @Mock
    private RuntimeContext runtimeContext;

    private UserConfiguration userConfiguration;

    @Before
    public void setup() {
        initMocks(this);
        this.userConfiguration = new UserConfiguration(parameterTool);
        when(parameterTool.getLong(Constants.METRIC_TELEMETRY_SHUTDOWN_PERIOD_MS_KEY, Constants.METRIC_TELEMETRY_SHUTDOWN_PERIOD_MS_DEFAULT)).thenReturn(0L);
        when(parameterTool.getBoolean(METRIC_TELEMETRY_ENABLE_KEY, METRIC_TELEMETRY_ENABLE_VALUE_DEFAULT)).thenReturn(true);
    }

    @Test
    public void shouldReturnErrorTelemetryFormConfigOnly() {
        ErrorReporter errorReporter = ErrorReporterFactory.getErrorReporter(runtimeContext, userConfiguration);
        assertEquals(errorReporter.getClass(), ErrorStatsReporter.class);
    }

    @Test
    public void shouldReturnErrorStatsReporterIfTelemetryEnabled() {
        ErrorReporter errorReporter = ErrorReporterFactory.getErrorReporter(runtimeContext, true, 0L);
        assertEquals(errorReporter.getClass(), ErrorStatsReporter.class);
    }

    @Test
    public void shouldReturnNoOpReporterIfTelemetryDisabled() {
        ErrorReporter errorReporter = ErrorReporterFactory.getErrorReporter(runtimeContext, false, 0L);
        assertEquals(errorReporter.getClass(), NoOpErrorReporter.class);
    }
}
