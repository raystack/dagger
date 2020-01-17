package com.gojek.daggers.postProcessors.external.http;

import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.daggers.metrics.telemetry.TelemetrySubscriber;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.MockitoAnnotations.initMocks;

public class HttpStreamDecoratorTest {
    @Mock
    private StencilClientOrchestrator stencilClientOrchestrator;

    @Mock
    private HttpSourceConfig httpSourceConfig;


    @Mock
    private TelemetrySubscriber telemetrySubscriber;

    private boolean telemetryEnabled;
    private long shutDownPeriod;
    @Before
    public void setUp() {
        initMocks(this);
        telemetryEnabled = true;
        shutDownPeriod = 0L;
    }

    @Test
    public void canDecorateHttpAsync() {
        HttpStreamDecorator httpStreamDecorator = new HttpStreamDecorator(httpSourceConfig, stencilClientOrchestrator, null, telemetrySubscriber, telemetryEnabled, shutDownPeriod);
        assertTrue(httpStreamDecorator.canDecorate());
    }

    @Test
    public void shouldNotDecorateOtherThanHttpAsync() {
        HttpStreamDecorator httpStreamDecorator = new HttpStreamDecorator(null, stencilClientOrchestrator, null, telemetrySubscriber, telemetryEnabled, shutDownPeriod);
        assertFalse(httpStreamDecorator.canDecorate());
    }
}