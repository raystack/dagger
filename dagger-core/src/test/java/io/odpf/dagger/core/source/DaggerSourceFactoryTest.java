package io.odpf.dagger.core.source;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.core.source.kafka.DaggerKafkaSource;
import io.odpf.dagger.core.source.kafka.DaggerOldKafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.HashMap;
import java.util.Properties;
import java.util.regex.Pattern;

import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class DaggerSourceFactoryTest {

    @Mock
    private StreamConfig streamConfig;

    @Mock
    private KafkaDeserializationSchema deserializationSchema;

    @Mock
    private Configuration configuration;

    @Before
    public void setup() {
        initMocks(this);
        HashMap<String, String> kafkaPropMap = new HashMap<>();
        kafkaPropMap.put("group.id", "dummy-consumer-group");
        kafkaPropMap.put("bootstrap.servers", "localhost:9092");

        Properties properties = new Properties();
        properties.putAll(kafkaPropMap);

        when(streamConfig.getDataType()).thenReturn("PROTO");
        when(streamConfig.getProtoClass()).thenReturn("com.tests.TestMessage");
        when(streamConfig.getEventTimestampFieldIndex()).thenReturn("1");
        when(streamConfig.getKafkaProps(any())).thenReturn(properties);
        when(streamConfig.getStartingOffset()).thenReturn(OffsetsInitializer.committedOffsets(OffsetResetStrategy.valueOf("LATEST")));
        when(streamConfig.getSchemaTable()).thenReturn("test-table");
        when(streamConfig.getTopicPattern()).thenReturn(Pattern.compile("test"));
    }

    @Test
    public void shouldCreateOldKafkaSourceIfGivenInConfig() {
        when(streamConfig.getSourceType()).thenReturn("OLD_KAFKA_SOURCE");
        DaggerSource daggerSource = DaggerSourceFactory.createDaggerSource(streamConfig, deserializationSchema, configuration);
        Assert.assertTrue(daggerSource instanceof DaggerOldKafkaSource);
    }

    @Test
    public void shouldCreateKafkaSourceIfGivenInConfig() {
        when(streamConfig.getSourceType()).thenReturn("KAFKA_SOURCE");
        DaggerSource daggerSource = DaggerSourceFactory.createDaggerSource(streamConfig, deserializationSchema, configuration);
        Assert.assertTrue(daggerSource instanceof DaggerKafkaSource);
    }

    @Test
    public void shouldThrowExceptionIfNotSupportedSourceGiven() {
        when(streamConfig.getSourceType()).thenReturn("TEST");
        assertThrows(IllegalArgumentException.class,
                () -> DaggerSourceFactory.createDaggerSource(streamConfig, deserializationSchema, configuration));
    }
}
