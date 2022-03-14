package io.odpf.dagger.core.source;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.serde.DaggerDeserializer;
import io.odpf.dagger.common.serde.parquet.deserialization.SimpleGroupDeserializer;
import io.odpf.dagger.common.serde.proto.deserialization.ProtoDeserializer;
import io.odpf.dagger.core.exception.DaggerConfigurationException;
import io.odpf.dagger.core.source.kafka.KafkaSourceFactory;
import io.odpf.dagger.core.source.parquet.ParquetFileSourceFactory;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static io.odpf.dagger.core.source.SourceName.KAFKA;
import static io.odpf.dagger.core.source.SourceName.PARQUET;
import static io.odpf.dagger.core.source.SourceType.BOUNDED;
import static io.odpf.dagger.core.source.SourceType.UNBOUNDED;
import static org.junit.Assert.*;
import static org.mockito.BDDMockito.given;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.powermock.api.mockito.PowerMockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({SourceName.class, ParquetFileSourceFactory.class, KafkaSourceFactory.class, KafkaRecordDeserializationSchema.class})
public class SourceFactoryTest {
    @Mock
    private StreamConfig streamConfig;

    @Mock
    private Configuration configuration;

    @Mock
    private SourceName invalidSourceName;

    @Mock
    private DaggerDeserializer<Row> daggerDeserializer;

    @Mock
    private SimpleGroupDeserializer simpleGroupDeserializer;

    @Mock
    ProtoDeserializer protoDeserializer;

    @Mock
    private FileSource parquetFileSource;

    @Mock
    private KafkaRecordDeserializationSchema<Row> kafkaRecordDeserializationSchema;

    @Mock
    private KafkaSource kafkaSource;

    @Before
    public void setup() {
        initMocks(this);

        mockStatic(KafkaSourceFactory.class);
        mockStatic(ParquetFileSourceFactory.class);
        mockStatic(KafkaRecordDeserializationSchema.class);

        invalidSourceName = PowerMockito.mock(SourceName.class);
        given(invalidSourceName.name()).willReturn("INVALID_SOURCE_NAME");
        given(invalidSourceName.ordinal()).willReturn(2);
        mockStatic(SourceName.class);
        PowerMockito.when(SourceName.values()).thenReturn(new SourceName[]{KAFKA, PARQUET, invalidSourceName});
    }

    @Test
    public void shouldThrowExceptionIfSourceNameNotSupported() {
        SourceDetails sourceDetails = new SourceDetails(invalidSourceName, BOUNDED);
        DaggerConfigurationException exception = assertThrows(DaggerConfigurationException.class,
                () -> SourceFactory.create(sourceDetails, streamConfig, configuration, daggerDeserializer));

        assertEquals("Invalid stream configuration: No suitable Flink DataSource could be constructed for source INVALID_SOURCE_NAME",
                exception.getMessage());
    }

    @Test
    public void shouldReturnInstanceOfFileSourceWhenSourceNameIsConfiguredAsParquet() throws Exception {
        SourceDetails sourceDetails = new SourceDetails(PARQUET, BOUNDED);
        doReturn(parquetFileSource).when(ParquetFileSourceFactory.class,
                "getFileSource", BOUNDED, streamConfig, configuration, simpleGroupDeserializer);

        Source expectedSource = SourceFactory.create(sourceDetails, streamConfig, configuration, simpleGroupDeserializer);

        assertTrue(expectedSource instanceof FileSource);
    }

    @Test
    public void shouldReturnInstanceOfKafkaSourceWhenSourceNameIsConfiguredAsKafka() throws Exception {
        SourceDetails sourceDetails = new SourceDetails(KAFKA, UNBOUNDED);
        doReturn(kafkaRecordDeserializationSchema).when(KafkaRecordDeserializationSchema.class,
                "of", protoDeserializer);
        doReturn(kafkaSource).when(KafkaSourceFactory.class,
                "getSource", UNBOUNDED, streamConfig, configuration, kafkaRecordDeserializationSchema);

        Source expectedSource = SourceFactory.create(sourceDetails, streamConfig, configuration, protoDeserializer);

        assertTrue(expectedSource instanceof KafkaSource);
    }
}