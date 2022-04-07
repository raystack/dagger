package io.odpf.dagger.core.source.kafka;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.core.source.StreamConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.types.Row;
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

public class DaggerOldKafkaSourceTest {

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
    private FlinkKafkaConsumerCustom flinkKafkaConsumerCustom;

    @Mock
    private FlinkKafkaConsumerBase<Row> kafkaConsumerBaseWithWatermarks;

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
    }

    @Test
    public void shouldHandleOldKafkaSource() {
        when(streamConfig.getSourceType()).thenReturn("OLD_KAFKA_SOURCE");
        DaggerOldKafkaSource daggerOldKafkaSource = new DaggerOldKafkaSource(streamConfig, deserializationSchema, configuration);
        Assert.assertTrue(daggerOldKafkaSource.canHandle());
    }

    @Test
    public void shouldNotHandleOtherSourceType() {
        when(streamConfig.getSourceType()).thenReturn("KAFKA_SOURCE");
        DaggerOldKafkaSource daggerOldKafkaSource = new DaggerOldKafkaSource(streamConfig, deserializationSchema, configuration);
        Assert.assertFalse(daggerOldKafkaSource.canHandle());
    }

    @Test
    public void shouldRegisterOnExecutionEnvironment() {
        when(flinkKafkaConsumerCustom.assignTimestampsAndWatermarks(watermarkStrategy)).thenReturn(kafkaConsumerBaseWithWatermarks);
        DaggerOldKafkaSource daggerOldKafkaSource = new DaggerOldKafkaSourceStub(streamConfig, deserializationSchema, configuration);
        daggerOldKafkaSource.register(environment, watermarkStrategy, "data_stream_0");

        verify(flinkKafkaConsumerCustom, times(1)).assignTimestampsAndWatermarks(watermarkStrategy);
        verify(environment, times(1)).addSource(kafkaConsumerBaseWithWatermarks);
    }

    class DaggerOldKafkaSourceStub extends DaggerOldKafkaSource {

        DaggerOldKafkaSourceStub(StreamConfig streamConfig, KafkaDeserializationSchema deserializationSchema, Configuration configuration) {
            super(streamConfig, deserializationSchema, configuration);
        }

        @Override
        FlinkKafkaConsumerCustom getFlinkKafkaConsumerCustom() {
            return flinkKafkaConsumerCustom;
        }
    }
}
