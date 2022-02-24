package io.odpf.dagger.core.processors.external.pg;

import io.odpf.dagger.core.processors.external.ExternalMetricConfig;
import io.odpf.dagger.core.processors.common.SchemaConfig;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.HashMap;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PgStreamDecoratorTest {


    @Mock
    private ExternalMetricConfig externalMetricConfig;

    @Mock
    private SchemaConfig schemaConfig;

    private PgSourceConfig pgSourceConfig;

    @Before
    public void setUp() {
        pgSourceConfig = new PgSourceConfigBuilder()
                .setHost("localhost")
                .setPort("9200")
                .setUser("")
                .setPassword("")
                .setDatabase("")
                .setType("")
                .setCapacity("20")
                .setStreamTimeout("5000")
                .setOutputMapping(new HashMap<>())
                .setConnectTimeout("5000")
                .setIdleTimeout("5000")
                .setQueryVariables("")
                .setQueryPattern("")
                .setFailOnErrors(true)
                .setMetricId("test-metric")
                .setRetainResponseType(false)
                .createPgSourceConfig();
    }

    @Test
    public void canDecorateStreamWhenConfigIsPresent() {
        PgStreamDecorator pgStreamDecorator = new PgStreamDecorator(pgSourceConfig, externalMetricConfig, schemaConfig);

        assertTrue(pgStreamDecorator.canDecorate());
    }

    @Test
    public void cannotDecorateStreamWhenConfigIsNull() {
        PgStreamDecorator pgStreamDecorator = new PgStreamDecorator(null, externalMetricConfig, schemaConfig);

        assertFalse(pgStreamDecorator.canDecorate());
    }
}
