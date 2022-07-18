package io.odpf.dagger.core.source.parquet;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.core.metrics.reporters.statsd.SerializedStatsDReporterSupplier;
import io.odpf.dagger.common.serde.DaggerDeserializer;
import io.odpf.dagger.common.serde.parquet.deserialization.SimpleGroupDeserializer;
import io.odpf.dagger.common.serde.proto.deserialization.ProtoDeserializer;
import io.odpf.dagger.core.exception.DaggerConfigurationException;
import io.odpf.dagger.core.source.config.StreamConfig;
import io.odpf.dagger.core.source.config.models.SourceDetails;
import io.odpf.dagger.core.source.config.models.SourceName;
import io.odpf.dagger.core.source.config.models.SourceType;
import io.odpf.depot.metrics.StatsDReporter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static io.odpf.dagger.core.source.parquet.SourceParquetReadOrderStrategy.EARLIEST_INDEX_FIRST;
import static io.odpf.dagger.core.source.parquet.SourceParquetReadOrderStrategy.EARLIEST_TIME_URL_FIRST;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

@PrepareForTest(FileSource.class)
@RunWith(PowerMockRunner.class)
public class ParquetDaggerSourceTest {
    @Mock
    private Configuration configuration;

    @Mock
    private StreamConfig streamConfig;

    @Mock
    private DaggerDeserializer<Row> daggerDeserializer;

    @Mock
    private WatermarkStrategy<Row> strategy;

    @Mock
    private StreamExecutionEnvironment streamExecutionEnvironment;

    @Mock
    private StatsDReporter statsDReporter;

    private final SerializedStatsDReporterSupplier statsDReporterSupplierMock = () -> statsDReporter;

    private FileSource<Row> fileSource;

    @Before
    public void setup() {
        initMocks(this);
        daggerDeserializer = Mockito.mock(SimpleGroupDeserializer.class);
        /* FileSource is a final class and hence cannot be mocked using vanilla Mockito */
        fileSource = PowerMockito.mock(FileSource.class);
    }

    @Test
    public void shouldBeAbleToBuildSourceIfSourceDetailsIsBoundedParquetAndDaggerDeserializerIsSimpleGroupDeserializer() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.PARQUET_SOURCE, SourceType.BOUNDED)});
        ParquetDaggerSource daggerSource = new ParquetDaggerSource(streamConfig, configuration, daggerDeserializer, statsDReporterSupplierMock);

        assertTrue(daggerSource.canBuild());
    }

    @Test
    public void shouldNotBeAbleToBuildSourceIfSourceDetailsContainsMultipleBackToBackSources() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.PARQUET_SOURCE, SourceType.BOUNDED),
                new SourceDetails(SourceName.PARQUET_SOURCE, SourceType.BOUNDED)});
        ParquetDaggerSource daggerSource = new ParquetDaggerSource(streamConfig, configuration, daggerDeserializer, statsDReporterSupplierMock);

        assertFalse(daggerSource.canBuild());
    }

    @Test
    public void shouldNotBeAbleToBuildSourceIfSourceNameIsUnsupported() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.KAFKA_CONSUMER, SourceType.BOUNDED)});
        ParquetDaggerSource daggerSource = new ParquetDaggerSource(streamConfig, configuration, daggerDeserializer, statsDReporterSupplierMock);

        assertFalse(daggerSource.canBuild());
    }

    @Test
    public void shouldNotBeAbleToBuildSourceIfSourceTypeIsUnsupported() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.PARQUET_SOURCE, SourceType.UNBOUNDED)});
        ParquetDaggerSource daggerSource = new ParquetDaggerSource(streamConfig, configuration, daggerDeserializer, statsDReporterSupplierMock);

        assertFalse(daggerSource.canBuild());
    }

    @Test
    public void shouldNotBeAbleToBuildSourceIfDeserializerTypeIsUnsupported() {
        DaggerDeserializer<Row> unsupportedDeserializer = Mockito.mock(ProtoDeserializer.class);
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.PARQUET_SOURCE, SourceType.BOUNDED)});
        ParquetDaggerSource daggerSource = new ParquetDaggerSource(streamConfig, configuration, unsupportedDeserializer, statsDReporterSupplierMock);

        assertFalse(daggerSource.canBuild());
    }

    @Test
    public void shouldBeAbleToRegisterSourceWithExecutionEnvironmentForCorrectConfiguration() {
        ParquetDaggerSource daggerSource = new ParquetDaggerSource(streamConfig, configuration, daggerDeserializer, statsDReporterSupplierMock);
        ParquetDaggerSource daggerSourceSpy = Mockito.spy(daggerSource);
        doReturn(fileSource).when(daggerSourceSpy).buildFileSource();
        when(streamConfig.getSchemaTable()).thenReturn("data_stream_0");

        daggerSourceSpy.register(streamExecutionEnvironment, strategy);

        verify(streamExecutionEnvironment, times(1)).fromSource(fileSource, strategy, "data_stream_0");
    }

    @Test
    public void shouldUseStreamConfigurationToBuildTheFileSource() {
        /* the below call mocks ensure that the function calls are indeed made to build the source and the code compiles */
        when(streamConfig.getSchemaTable()).thenReturn("data_stream_0");
        when(streamConfig.getParquetFilesReadOrderStrategy()).thenReturn(EARLIEST_TIME_URL_FIRST);
        when(streamConfig.getParquetFilePaths()).thenReturn(new String[]{"gs://sshsh", "gs://shadd"});

        ParquetDaggerSource daggerSource = new ParquetDaggerSource(streamConfig, configuration, daggerDeserializer, statsDReporterSupplierMock);

        daggerSource.register(streamExecutionEnvironment, strategy);
    }

    @Test
    public void shouldThrowRuntimeExceptionAndReportErrorIfReadOrderStrategyIsNotSupported() {
        when(streamConfig.getParquetFilesReadOrderStrategy()).thenReturn(EARLIEST_INDEX_FIRST);
        when(streamConfig.getParquetFilePaths()).thenReturn(new String[]{"gs://sshsh", "gs://shadd"});

        ParquetDaggerSource daggerSource = new ParquetDaggerSource(streamConfig, configuration, daggerDeserializer, statsDReporterSupplierMock);

        assertThrows(DaggerConfigurationException.class, () -> daggerSource.register(streamExecutionEnvironment, strategy));
        verify(statsDReporter, times(1))
                .captureCount("fatal.exception", 1L, "fatal_exception_type=" + DaggerConfigurationException.class.getName());
    }
}
