package org.raystack.dagger.core.processors.common;

import org.raystack.dagger.common.configuration.Configuration;
import org.raystack.dagger.common.core.StencilClientOrchestrator;
import org.raystack.dagger.core.processors.ColumnNameManager;
import org.raystack.dagger.core.utils.Constants;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.raystack.dagger.common.core.Constants.INPUT_STREAMS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class SchemaConfigTest {
    @Mock
    private StencilClientOrchestrator stencilClientOrchestrator;

    @Mock
    private ColumnNameManager columnNameManager;

    @Mock
    private Configuration configuration;


    @Before
    public void setup() {
        initMocks(this);
        String streams = "[{\"SOURCE_KAFKA_TOPIC_NAMES\":\"topic-name\",\"INPUT_SCHEMA_TABLE\":\"booking\",\"INPUT_SCHEMA_PROTO_CLASS\":\"InputProtoMessage\",\"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\":\"41\",\"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\":\"10.1.2.3:9092\",\"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\":\"\",\"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\":\"latest\",\"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\":\"test-config\",\"SOURCE_KAFKA_NAME\":\"test\"}]";
        when(configuration.getString(INPUT_STREAMS, "")).thenReturn(streams);
        when(configuration.getString(Constants.SINK_KAFKA_PROTO_MESSAGE_KEY, "")).thenReturn("OutputProtoMessage");
    }

    @Test
    public void shouldReturnStencilOrchestrator() {
        SchemaConfig schemaConfig = new SchemaConfig(configuration, stencilClientOrchestrator, columnNameManager);
        assertEquals(stencilClientOrchestrator, schemaConfig.getStencilClientOrchestrator());
    }

    @Test
    public void shouldReturnColumnNameManager() {
        SchemaConfig schemaConfig = new SchemaConfig(configuration, stencilClientOrchestrator, columnNameManager);
        Assert.assertEquals(columnNameManager, schemaConfig.getColumnNameManager());
    }

    @Test
    public void shouldReturnInputProtoClasses() {
        SchemaConfig schemaConfig = new SchemaConfig(configuration, stencilClientOrchestrator, columnNameManager);
        assertArrayEquals(new String[]{"InputProtoMessage"}, schemaConfig.getInputProtoClasses());
    }

    @Test
    public void shouldReturnOutputProtoClassName() {
        SchemaConfig schemaConfig = new SchemaConfig(configuration, stencilClientOrchestrator, columnNameManager);
        assertEquals("OutputProtoMessage", schemaConfig.getOutputProtoClassName());
    }

}
