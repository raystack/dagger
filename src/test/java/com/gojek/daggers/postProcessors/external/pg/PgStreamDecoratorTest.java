package com.gojek.daggers.postProcessors.external.pg;

import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.daggers.postProcessors.external.ExternalMetricConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.HashMap;

import static org.mockito.Mockito.mock;

public class PgStreamDecoratorTest {

    private StencilClientOrchestrator stencilClientOrchestrator;

    @Mock
    private ExternalMetricConfig externalMetricConfig;
    private PgSourceConfig pgSourceConfig;
    private String[] inputProtoClasses;

    @Before
    public void setUp() {
        stencilClientOrchestrator = mock(StencilClientOrchestrator.class);
        inputProtoClasses = new String[]{"com.gojek.esb.booking.GoFoodBookingLogMessage"};
        pgSourceConfig = new PgSourceConfig("localhost", "9200", "",
                "", "", "",
                "20", "5000", new HashMap<>(), "5000", "5000", "", "", true, "test-metric");
    }

    @Test
    public void canDecorateStreamWhenConfigIsPresent() {
        PgStreamDecorator pgStreamDecorator = new PgStreamDecorator(pgSourceConfig, stencilClientOrchestrator, null, inputProtoClasses, externalMetricConfig);

        Assert.assertTrue(pgStreamDecorator.canDecorate());
    }

    @Test
    public void cannotDecorateStreamWhenConfigIsNull() {
        PgStreamDecorator pgStreamDecorator = new PgStreamDecorator(null, stencilClientOrchestrator, null, inputProtoClasses, externalMetricConfig);

        Assert.assertFalse(pgStreamDecorator.canDecorate());
    }
}