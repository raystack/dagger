package io.odpf.dagger.core.processors.external.pg;

import io.odpf.dagger.core.processors.external.ExternalMetricConfig;
import io.odpf.dagger.core.processors.external.SchemaConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.HashMap;

public class PgStreamDecoratorTest {


    @Mock
    private ExternalMetricConfig externalMetricConfig;

    @Mock
    private SchemaConfig schemaConfig;

    private PgSourceConfig pgSourceConfig;

    @Before
    public void setUp() {
        pgSourceConfig = new PgSourceConfig("localhost", "9200", "",
                "", "", "",
                "20", "5000", new HashMap<>(), "5000", "5000", "", "", true, "test-metric", false);
    }

    @Test
    public void canDecorateStreamWhenConfigIsPresent() {
        PgStreamDecorator pgStreamDecorator = new PgStreamDecorator(pgSourceConfig, externalMetricConfig, schemaConfig);

        Assert.assertTrue(pgStreamDecorator.canDecorate());
    }

    @Test
    public void cannotDecorateStreamWhenConfigIsNull() {
        PgStreamDecorator pgStreamDecorator = new PgStreamDecorator(null, externalMetricConfig, schemaConfig);

        Assert.assertFalse(pgStreamDecorator.canDecorate());
    }
}
