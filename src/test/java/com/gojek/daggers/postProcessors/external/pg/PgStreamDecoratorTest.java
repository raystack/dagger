package com.gojek.daggers.postProcessors.external.pg;

import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.daggers.metrics.telemetry.TelemetrySubscriber;
import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.HashMap;

import static org.mockito.Mockito.mock;

public class PgStreamDecoratorTest {

    private StencilClientOrchestrator stencilClientOrchestrator;
    private boolean telemetryEnabled;
    private long shutDownPeriod;

    @Mock
    private TelemetrySubscriber telemetrySubscriber;
    private PgSourceConfig pgSourceConfig;
    private String metricId;

    @Before
    public void setUp() {
        stencilClientOrchestrator = mock(StencilClientOrchestrator.class);
        telemetryEnabled = true;
        shutDownPeriod = 0L;
        pgSourceConfig = new PgSourceConfig("localhost", "9200", "",
                "", "", "",
                "20", "5000", new HashMap<>(), "5000", "5000", "", "", true, metricId);
    }

    @Test
    public void canDecorateStreamWhenConfigIsPresent() {
        PgStreamDecorator pgStreamDecorator = new PgStreamDecorator(pgSourceConfig, stencilClientOrchestrator, new ColumnNameManager(new String[4], new ArrayList<>()), telemetrySubscriber, telemetryEnabled, shutDownPeriod);

        Assert.assertTrue(pgStreamDecorator.canDecorate());
    }

    @Test
    public void cannotDecorateStreamWhenConfigIsNull() {
        PgStreamDecorator pgStreamDecorator = new PgStreamDecorator(null, stencilClientOrchestrator, new ColumnNameManager(new String[4], new ArrayList<>()), telemetrySubscriber, telemetryEnabled, shutDownPeriod);

        Assert.assertFalse(pgStreamDecorator.canDecorate());
    }
}