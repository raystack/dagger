package org.raystack.dagger.core.source.flinkkafkaconsumer;

import org.raystack.dagger.common.configuration.Configuration;
import org.raystack.dagger.common.serde.DaggerDeserializer;
import org.raystack.dagger.common.serde.json.deserialization.JsonDeserializer;
import org.raystack.dagger.common.serde.parquet.deserialization.SimpleGroupDeserializer;
import org.raystack.dagger.core.source.config.StreamConfig;
import org.raystack.dagger.core.source.config.models.SourceDetails;
import org.raystack.dagger.core.source.config.models.SourceName;
import org.raystack.dagger.core.source.config.models.SourceType;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import static org.junit.Assert.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class FlinkKafkaConsumerDaggerSourceTest {

    @Mock
    private Configuration configuration;

    @Mock
    private StreamConfig streamConfig;

    @Mock
    private DaggerDeserializer<Row> daggerDeserializer;

    @Mock
    private FlinkKafkaConsumerCustom flinkKafkaConsumerCustom;

    @Mock
    private WatermarkStrategy<Row> watermarkStrategy;

    @Mock
    private StreamExecutionEnvironment streamExecutionEnvironment;

    @Before
    public void setup() {
        initMocks(this);
        daggerDeserializer = Mockito.mock(JsonDeserializer.class);
    }

    @Test
    public void shouldBeAbleToBuildSourceIfSourceDetailsIsUnboundedKafkaConsumerAndDaggerDeserializerIsKafkaDeserializationSchema() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.KAFKA_CONSUMER, SourceType.UNBOUNDED)});
        FlinkKafkaConsumerDaggerSource daggerSource = new FlinkKafkaConsumerDaggerSource(streamConfig, configuration, daggerDeserializer);

        assertTrue(daggerSource.canBuild());
    }

    @Test
    public void shouldNotBeAbleToBuildSourceIfSourceDetailsContainsMultipleBackToBackSources() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.KAFKA_CONSUMER, SourceType.UNBOUNDED),
                new SourceDetails(SourceName.KAFKA_CONSUMER, SourceType.UNBOUNDED)});
        FlinkKafkaConsumerDaggerSource daggerSource = new FlinkKafkaConsumerDaggerSource(streamConfig, configuration, daggerDeserializer);

        assertFalse(daggerSource.canBuild());
    }

    @Test
    public void shouldNotBeAbleToBuildSourceIfSourceNameIsUnsupported() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.PARQUET_SOURCE, SourceType.UNBOUNDED)});
        FlinkKafkaConsumerDaggerSource daggerSource = new FlinkKafkaConsumerDaggerSource(streamConfig, configuration, daggerDeserializer);

        assertFalse(daggerSource.canBuild());
    }

    @Test
    public void shouldNotBeAbleToBuildSourceIfSourceTypeIsUnsupported() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.KAFKA_CONSUMER, SourceType.BOUNDED)});
        FlinkKafkaConsumerDaggerSource daggerSource = new FlinkKafkaConsumerDaggerSource(streamConfig, configuration, daggerDeserializer);

        assertFalse(daggerSource.canBuild());
    }

    @Test
    public void shouldNotBeAbleToBuildSourceIfDeserializerTypeIsUnsupported() {
        DaggerDeserializer<Row> unsupportedDeserializer = Mockito.mock(SimpleGroupDeserializer.class);
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.KAFKA_CONSUMER, SourceType.UNBOUNDED)});
        FlinkKafkaConsumerDaggerSource daggerSource = new FlinkKafkaConsumerDaggerSource(streamConfig, configuration, unsupportedDeserializer);

        assertFalse(daggerSource.canBuild());
    }

    @Test
    public void shouldBeAbleToRegisterSourceWithExecutionEnvironmentForCorrectConfiguration() {
        FlinkKafkaConsumerDaggerSource daggerSource = new FlinkKafkaConsumerDaggerSource(streamConfig, configuration, daggerDeserializer);
        FlinkKafkaConsumerDaggerSource daggerSourceSpy = Mockito.spy(daggerSource);
        doReturn(flinkKafkaConsumerCustom).when(daggerSourceSpy).buildSource();
        when(flinkKafkaConsumerCustom.assignTimestampsAndWatermarks(watermarkStrategy)).thenReturn(flinkKafkaConsumerCustom);

        daggerSourceSpy.register(streamExecutionEnvironment, watermarkStrategy);

        verify(flinkKafkaConsumerCustom, times(1)).assignTimestampsAndWatermarks(watermarkStrategy);
        verify(streamExecutionEnvironment, times(1)).addSource(flinkKafkaConsumerCustom);
    }
}
