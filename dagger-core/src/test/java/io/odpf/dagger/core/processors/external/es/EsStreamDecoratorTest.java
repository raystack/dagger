package io.odpf.dagger.core.processors.external.es;

import io.odpf.dagger.core.processors.external.ExternalMetricConfig;
import io.odpf.dagger.core.processors.external.SchemaConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.HashMap;

public class EsStreamDecoratorTest {

    private EsSourceConfig esSourceConfig;

    @Mock
    private ExternalMetricConfig externalMetricConfig;

    @Mock
    private SchemaConfig schemaConfig;

    @Before
    public void setUp() {
        esSourceConfig = new EsSourceConfig("localhost", "9200", "", "", "",
                "driver_id", "io.odpf.TestProtoMessage", "30",
                "5000", "5000", "5000", "5000", false, new HashMap<>(), "metricId_01", false);
    }

    @Test
    public void canDecorateStreamWhenConfigIsPresent() {
        EsStreamDecorator esStreamDecorator = new EsStreamDecorator(esSourceConfig, externalMetricConfig, schemaConfig);

        Assert.assertTrue(esStreamDecorator.canDecorate());
    }

    @Test
    public void cannotDecorateStreamWhenConfigIsNull() {
        EsStreamDecorator esStreamDecorator = new EsStreamDecorator(null, externalMetricConfig, schemaConfig);

        Assert.assertFalse(esStreamDecorator.canDecorate());
    }
}
