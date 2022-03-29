package io.odpf.dagger.core.streamtype;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.serde.proto.deserialization.ProtoDeserializer;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.core.source.SourceDetails;
import io.odpf.dagger.core.source.StreamConfig;
import io.odpf.stencil.client.StencilClient;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.HashMap;
import java.util.Properties;

import static io.odpf.dagger.common.serde.DataTypes.PROTO;
import static io.odpf.dagger.core.source.SourceName.KAFKA;
import static io.odpf.dagger.core.source.SourceName.PARQUET;
import static io.odpf.dagger.core.source.SourceType.BOUNDED;
import static io.odpf.dagger.core.source.SourceType.UNBOUNDED;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class KafkaSourceProtoTypeTest {
    @Mock
    private StreamConfig streamConfig;

    @Mock
    private Configuration configuration;

    @Mock
    private StencilClientOrchestrator stencilClientOrchestrator;

    @Mock
    private StencilClient stencilClient;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldReturnTrueIfTheStreamTypeCanBeBuiltFromConfigs() {
        KafkaSourceProtoType.KafkaSourceProtoTypeBuilder kafkaSourceProtoTypeBuilder = new KafkaSourceProtoType.KafkaSourceProtoTypeBuilder(streamConfig, configuration, stencilClientOrchestrator);
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(KAFKA, UNBOUNDED)});
        when(streamConfig.getDataType()).thenReturn("PROTO");

        boolean canBuild = kafkaSourceProtoTypeBuilder.canBuild();

        assertTrue(canBuild);
    }

    @Test
    public void shouldReturnFalseIfTheSourceNameIsNotSupported() {
        KafkaSourceProtoType.KafkaSourceProtoTypeBuilder kafkaSourceProtoTypeBuilder = new KafkaSourceProtoType.KafkaSourceProtoTypeBuilder(streamConfig, configuration, stencilClientOrchestrator);
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(PARQUET, UNBOUNDED)});
        when(streamConfig.getDataType()).thenReturn("PROTO");

        boolean canBuild = kafkaSourceProtoTypeBuilder.canBuild();

        assertFalse(canBuild);
    }

    @Test
    public void shouldReturnFalseIfMultipleBackToBackSourcesAreConfigured() {
        KafkaSourceProtoType.KafkaSourceProtoTypeBuilder kafkaSourceProtoTypeBuilder = new KafkaSourceProtoType.KafkaSourceProtoTypeBuilder(streamConfig, configuration, stencilClientOrchestrator);
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(KAFKA, BOUNDED), new SourceDetails(KAFKA, UNBOUNDED)});
        when(streamConfig.getDataType()).thenReturn("PROTO");

        boolean canBuild = kafkaSourceProtoTypeBuilder.canBuild();

        assertFalse(canBuild);
    }

    @Test
    public void shouldReturnFalseIfTheSourceTypeIsNotSupported() {
        KafkaSourceProtoType.KafkaSourceProtoTypeBuilder kafkaSourceProtoTypeBuilder = new KafkaSourceProtoType.KafkaSourceProtoTypeBuilder(streamConfig, configuration, stencilClientOrchestrator);
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(KAFKA, BOUNDED)});
        when(streamConfig.getDataType()).thenReturn("PROTO");

        boolean canBuild = kafkaSourceProtoTypeBuilder.canBuild();

        assertFalse(canBuild);
    }

    @Test
    public void shouldReturnFalseIfTheSchemaTypeIsNotSupported() {
        KafkaSourceProtoType.KafkaSourceProtoTypeBuilder kafkaSourceProtoTypeBuilder = new KafkaSourceProtoType.KafkaSourceProtoTypeBuilder(streamConfig, configuration, stencilClientOrchestrator);
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(KAFKA, UNBOUNDED)});
        when(streamConfig.getDataType()).thenReturn("JSON");

        boolean canBuild = kafkaSourceProtoTypeBuilder.canBuild();

        assertFalse(canBuild);
    }

    @Test
    public void shouldBuildAStreamTypeWithKafkaSourceAndProtoSchemaType() {
        KafkaSourceProtoType.KafkaSourceProtoTypeBuilder kafkaSourceProtoTypeBuilder = new KafkaSourceProtoType.KafkaSourceProtoTypeBuilder(streamConfig, configuration, stencilClientOrchestrator);
        HashMap<String, String> kafkaPropMap = new HashMap<>();
        kafkaPropMap.put("group.id", "dummy-consumer-group");
        kafkaPropMap.put("bootstrap.servers", "localhost:9092");

        Properties properties = new Properties();
        properties.putAll(kafkaPropMap);

        when(streamConfig.getKafkaProps(any())).thenReturn(properties);
        when(streamConfig.getEventTimestampFieldIndex()).thenReturn("5");
        when(streamConfig.getProtoClass()).thenReturn("com.tests.TestMessage");
        when(streamConfig.getStartingOffset()).thenReturn(OffsetsInitializer.committedOffsets(OffsetResetStrategy.valueOf("LATEST")));
        when(streamConfig.getSchemaTable()).thenReturn("test-table");
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(stencilClient.get("com.tests.TestMessage")).thenReturn(TestBookingLogMessage.getDescriptor());


        StreamType<Row> streamType = kafkaSourceProtoTypeBuilder.build();

        assertTrue(streamType.getSource() instanceof KafkaSource);
        assertEquals("test-table", streamType.getStreamName());
        assertTrue(streamType.getDeserializer() instanceof ProtoDeserializer);
        assertEquals(PROTO, streamType.getInputDataType());
    }
}