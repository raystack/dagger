package io.odpf.dagger.core.source.kafka;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.core.source.StreamConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.HashMap;
import java.util.Properties;
import java.util.regex.Pattern;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class DaggerKafkaSourceTest {
    @Mock
    private StreamConfig streamConfig;

    @Mock
    private KafkaDeserializationSchema deserializationSchema;

    @Mock
    private Configuration configuration;

    @Mock
    private StreamExecutionEnvironment environment;

    @Mock
    private WatermarkStrategy<Row> watermarkStrategy;

    @Mock
    private KafkaSource kafkaSource;

    @Before
    public void setUp() {
        initMocks(this);
        HashMap<String, String> kafkaPropMap = new HashMap<>();
        kafkaPropMap.put("group.id", "dummy-consumer-group");
        kafkaPropMap.put("bootstrap.servers", "localhost:9092");

        Properties properties = new Properties();
        properties.putAll(kafkaPropMap);
        when(streamConfig.getKafkaProps(any())).thenReturn(properties);
        when(streamConfig.getTopicPattern()).thenReturn(Pattern.compile("test"));
        when(streamConfig.getStartingOffset()).thenReturn(OffsetsInitializer.committedOffsets(OffsetResetStrategy.valueOf("LATEST")));
    }

    @Test
    public void shouldHandleKafkaSource() {
        when(streamConfig.getSourceType()).thenReturn("KAFKA_SOURCE");
        DaggerKafkaSource daggerKafkaSource = new DaggerKafkaSource(streamConfig, deserializationSchema, configuration);
        Assert.assertTrue(daggerKafkaSource.canHandle());
    }

    @Test
    public void shouldNotHandleOtherSourceType() {
        when(streamConfig.getSourceType()).thenReturn("OLD_KAFKA_SOURCE");
        DaggerKafkaSource daggerKafkaSource = new DaggerKafkaSource(streamConfig, deserializationSchema, configuration);
        Assert.assertFalse(daggerKafkaSource.canHandle());
    }

    @Test
    public void shouldRegisterOnExecutionEnvironment() {
        DaggerKafkaSource daggerKafkaSource = new DaggerKafkaSourceStub(streamConfig, deserializationSchema, configuration);
        daggerKafkaSource.register(environment, watermarkStrategy, "data_stream_0");

        verify(environment, times(1)).fromSource(kafkaSource, watermarkStrategy, "data_stream_0");
    }

    class DaggerKafkaSourceStub extends DaggerKafkaSource {

        DaggerKafkaSourceStub(StreamConfig streamConfig, KafkaDeserializationSchema deserializationSchema, Configuration configuration) {
            super(streamConfig, deserializationSchema, configuration);
        }

        @Override
        KafkaSource getKafkaSource() {
            return kafkaSource;
        }
    }

}
