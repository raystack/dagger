package io.odpf.dagger.core.metrics.reporters;

import io.odpf.dagger.core.utils.Constants;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static io.odpf.dagger.core.utils.Constants.METRIC_TELEMETRY_ENABLE_KEY;
import static io.odpf.dagger.core.utils.Constants.METRIC_TELEMETRY_ENABLE_VALUE_DEFAULT;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class ErrorReporterFactoryTest {

    @Mock
    private Configuration configuration;

    @Mock
    private RuntimeContext runtimeContext;

    @Before
    public void setup() {
        initMocks(this);
        when(configuration.getLong(Constants.METRIC_TELEMETRY_SHUTDOWN_PERIOD_MS_KEY, Constants.METRIC_TELEMETRY_SHUTDOWN_PERIOD_MS_DEFAULT)).thenReturn(0L);
        when(configuration.getBoolean(METRIC_TELEMETRY_ENABLE_KEY, METRIC_TELEMETRY_ENABLE_VALUE_DEFAULT)).thenReturn(true);
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
