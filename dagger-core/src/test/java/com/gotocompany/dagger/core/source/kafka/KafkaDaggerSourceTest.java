package com.gotocompany.dagger.core.source.kafka;

import com.gotocompany.dagger.common.configuration.Configuration;
import com.gotocompany.dagger.common.serde.DaggerDeserializer;
import com.gotocompany.dagger.common.serde.parquet.deserialization.SimpleGroupDeserializer;
import com.gotocompany.dagger.common.serde.proto.deserialization.ProtoDeserializer;
import com.gotocompany.dagger.core.source.config.StreamConfig;
import com.gotocompany.dagger.core.source.config.models.SourceDetails;
import com.gotocompany.dagger.core.source.config.models.SourceName;
import com.gotocompany.dagger.core.source.config.models.SourceType;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;


import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class KafkaDaggerSourceTest {
    @Mock
    private Configuration configuration;

    @Mock
    private StreamConfig streamConfig;

    @Mock
    private DaggerDeserializer<Row> daggerDeserializer;

    @Mock
    private KafkaSource<Row> kafkaSource;

    @Mock
    private WatermarkStrategy<Row> strategy;

    @Mock
    private StreamExecutionEnvironment streamExecutionEnvironment;

    @Before
    public void setup() {
        initMocks(this);
        daggerDeserializer = Mockito.mock(ProtoDeserializer.class);
    }

    @Test
    public void shouldBeAbleToBuildSourceIfSourceDetailsIsUnboundedKafkaAndDaggerDeserializerIsKafkaDeserializationSchema() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.KAFKA_SOURCE, SourceType.UNBOUNDED)});
        KafkaDaggerSource daggerSource = new KafkaDaggerSource(streamConfig, configuration, daggerDeserializer);

        assertTrue(daggerSource.canBuild());
    }

    @Test
    public void shouldNotBeAbleToBuildSourceIfSourceDetailsContainsMultipleBackToBackSources() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.KAFKA_SOURCE, SourceType.UNBOUNDED),
                new SourceDetails(SourceName.KAFKA_SOURCE, SourceType.UNBOUNDED)});
        KafkaDaggerSource daggerSource = new KafkaDaggerSource(streamConfig, configuration, daggerDeserializer);

        assertFalse(daggerSource.canBuild());
    }

    @Test
    public void shouldNotBeAbleToBuildSourceIfSourceNameIsUnsupported() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.PARQUET_SOURCE, SourceType.UNBOUNDED)});
        KafkaDaggerSource daggerSource = new KafkaDaggerSource(streamConfig, configuration, daggerDeserializer);

        assertFalse(daggerSource.canBuild());
    }

    @Test
    public void shouldNotBeAbleToBuildSourceIfSourceTypeIsUnsupported() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.KAFKA_SOURCE, SourceType.BOUNDED)});
        KafkaDaggerSource daggerSource = new KafkaDaggerSource(streamConfig, configuration, daggerDeserializer);

        assertFalse(daggerSource.canBuild());
    }

    @Test
    public void shouldNotBeAbleToBuildSourceIfDeserializerTypeIsUnsupported() {
        DaggerDeserializer<Row> unsupportedDeserializer = Mockito.mock(SimpleGroupDeserializer.class);
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.KAFKA_SOURCE, SourceType.UNBOUNDED)});
        KafkaDaggerSource daggerSource = new KafkaDaggerSource(streamConfig, configuration, unsupportedDeserializer);

        assertFalse(daggerSource.canBuild());
    }

    @Test
    public void shouldBeAbleToRegisterSourceWithExecutionEnvironmentForCorrectConfiguration() {
        when(streamConfig.getSchemaTable()).thenReturn("data_stream_0");

        KafkaDaggerSource kafkaDaggerSource = new KafkaDaggerSource(streamConfig, configuration, daggerDeserializer);
        KafkaDaggerSource kafkaDaggerSourceSpy = Mockito.spy(kafkaDaggerSource);
        doReturn(kafkaSource).when(kafkaDaggerSourceSpy).buildSource();

        kafkaDaggerSourceSpy.register(streamExecutionEnvironment, strategy);

        verify(streamExecutionEnvironment, times(1)).fromSource(kafkaSource, strategy, "data_stream_0");
    }
}
