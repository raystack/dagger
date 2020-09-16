package com.gojek.daggers.postProcessors.external.http;

import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.daggers.postProcessors.external.ExternalMetricConfig;
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
    private ExternalMetricConfig externalMetricConfig;

    private String[] inputProtoClasses;

    @Before
    public void setUp() {
        inputProtoClasses = new String[]{"com.gojek.esb.booking.GoFoodBookingLogMessage"};
        initMocks(this);
    }

    @Test
    public void canDecorateHttpAsync() {
        HttpStreamDecorator httpStreamDecorator = new HttpStreamDecorator(httpSourceConfig, stencilClientOrchestrator, null, inputProtoClasses, externalMetricConfig);
        assertTrue(httpStreamDecorator.canDecorate());
    }

    @Test
    public void shouldNotDecorateOtherThanHttpAsync() {
        HttpStreamDecorator httpStreamDecorator = new HttpStreamDecorator(null, stencilClientOrchestrator, null, inputProtoClasses, externalMetricConfig);
        assertFalse(httpStreamDecorator.canDecorate());
    }
}