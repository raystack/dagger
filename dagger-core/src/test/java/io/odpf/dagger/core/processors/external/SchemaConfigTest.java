package io.odpf.dagger.core.processors.external;

import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.core.processors.ColumnNameManager;
import org.apache.flink.configuration.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static io.odpf.dagger.core.utils.Constants.INPUT_STREAMS;
import static io.odpf.dagger.core.utils.Constants.OUTPUT_PROTO_MESSAGE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.MockitoAnnotations.initMocks;

public class SchemaConfigTest {

    @Mock
    private StencilClientOrchestrator stencilClientOrchestrator;

    @Mock
    private ColumnNameManager columnNameManager;

    private Configuration configuration = new Configuration();


    @Before
    public void setup() {
        initMocks(this);
        String streams = "[{\"TOPIC_NAMES\":\"topic-name\",\"TABLE_NAME\":\"booking\",\"PROTO_CLASS_NAME\":\"InputProtoMessage\",\"EVENT_TIMESTAMP_FIELD_INDEX\":\"41\",\"KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\":\"10.1.2.3:9092\",\"KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\":\"\",\"KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\":\"latest\",\"KAFKA_CONSUMER_CONFIG_GROUP_ID\":\"test-config\",\"STREAM_NAME\":\"test\"}]";
        configuration.setString(INPUT_STREAMS, streams);
        configuration.setString(OUTPUT_PROTO_MESSAGE, "OutputProtoMessage");
    }

    @Test
    public void shouldReturnStencilOrchestrator() {
        SchemaConfig schemaConfig = new SchemaConfig(configuration, stencilClientOrchestrator, columnNameManager);
        assertEquals(stencilClientOrchestrator, schemaConfig.getStencilClientOrchestrator());
    }

    @Test
    public void shouldReturnColumnNameManager() {
        SchemaConfig schemaConfig = new SchemaConfig(configuration, stencilClientOrchestrator, columnNameManager);
        assertEquals(columnNameManager, schemaConfig.getColumnNameManager());
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
