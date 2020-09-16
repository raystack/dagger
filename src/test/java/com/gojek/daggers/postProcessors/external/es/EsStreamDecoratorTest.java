package com.gojek.daggers.postProcessors.external.es;

import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.daggers.postProcessors.external.ExternalMetricConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.HashMap;

import static org.mockito.Mockito.mock;

public class EsStreamDecoratorTest {

    private StencilClientOrchestrator stencilClientOrchestrator;
    private EsSourceConfig esSourceConfig;

    @Mock
    private ExternalMetricConfig externalMetricConfig;

    private String[] inputProtoClasses;

    @Before
    public void setUp() {
        stencilClientOrchestrator = mock(StencilClientOrchestrator.class);
        inputProtoClasses = new String[]{"com.gojek.esb.booking.GoFoodBookingLogMessage"};
        esSourceConfig = new EsSourceConfig("localhost", "9200", "",
                "driver_id", "com.gojek.esb.fraud.DriverProfileFlattenLogMessage", "30",
                "5000", "5000", "5000", "5000", false, new HashMap<>(), "metricId_01");
    }

    @Test
    public void canDecorateStreamWhenConfigIsPresent() {
        EsStreamDecorator esStreamDecorator = new EsStreamDecorator(esSourceConfig, stencilClientOrchestrator, null, inputProtoClasses, externalMetricConfig);

        Assert.assertTrue(esStreamDecorator.canDecorate());
    }

    @Test
    public void cannotDecorateStreamWhenConfigIsNull() {
        EsStreamDecorator esStreamDecorator = new EsStreamDecorator(null, stencilClientOrchestrator, null, inputProtoClasses, externalMetricConfig);

        Assert.assertFalse(esStreamDecorator.canDecorate());
    }
}