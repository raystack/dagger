package io.odpf.dagger.core.streamtype;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.serde.parquet.deserialization.SimpleGroupDeserializer;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.core.exception.DaggerConfigurationException;
import io.odpf.dagger.core.source.SourceDetails;
import io.odpf.dagger.core.source.StreamConfig;
import io.odpf.stencil.client.StencilClient;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static io.odpf.dagger.common.serde.DataTypes.PROTO;
import static io.odpf.dagger.core.source.SourceName.KAFKA;
import static io.odpf.dagger.core.source.SourceName.PARQUET;
import static io.odpf.dagger.core.source.SourceType.BOUNDED;
import static io.odpf.dagger.core.source.SourceType.UNBOUNDED;
import static io.odpf.dagger.core.source.parquet.SourceParquetReadOrderStrategy.EARLIEST_INDEX_FIRST;
import static io.odpf.dagger.core.source.parquet.SourceParquetReadOrderStrategy.EARLIEST_TIME_URL_FIRST;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class ParquetSourceProtoSchemaStreamTypeTest {
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
    public void shouldReturnFalseIfTheSourceNameIsNotSupported() {
        ParquetSourceProtoSchemaStreamType.ParquetSourceProtoSchemaStreamTypeBuilder parquetSourceProtoSchemaStreamTypeBuilder = new ParquetSourceProtoSchemaStreamType.ParquetSourceProtoSchemaStreamTypeBuilder(streamConfig, configuration, stencilClientOrchestrator);
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(KAFKA, BOUNDED)});
        when(streamConfig.getDataType()).thenReturn("PROTO");

        boolean canBuild = parquetSourceProtoSchemaStreamTypeBuilder.canBuild();

        assertFalse(canBuild);
    }

    @Test
    public void shouldReturnFalseIfMultipleBackToBackSourcesAreConfigured() {
        ParquetSourceProtoSchemaStreamType.ParquetSourceProtoSchemaStreamTypeBuilder parquetSourceProtoSchemaStreamTypeBuilder = new ParquetSourceProtoSchemaStreamType.ParquetSourceProtoSchemaStreamTypeBuilder(streamConfig, configuration, stencilClientOrchestrator);
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(PARQUET, BOUNDED), new SourceDetails(KAFKA, UNBOUNDED)});
        when(streamConfig.getDataType()).thenReturn("PROTO");

        boolean canBuild = parquetSourceProtoSchemaStreamTypeBuilder.canBuild();

        assertFalse(canBuild);
    }

    @Test
    public void shouldReturnFalseIfTheSourceTypeIsNotSupported() {
        ParquetSourceProtoSchemaStreamType.ParquetSourceProtoSchemaStreamTypeBuilder parquetSourceProtoSchemaStreamTypeBuilder = new ParquetSourceProtoSchemaStreamType.ParquetSourceProtoSchemaStreamTypeBuilder(streamConfig, configuration, stencilClientOrchestrator);
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(PARQUET, UNBOUNDED)});
        when(streamConfig.getDataType()).thenReturn("PROTO");

        boolean canBuild = parquetSourceProtoSchemaStreamTypeBuilder.canBuild();

        assertFalse(canBuild);
    }

    @Test
    public void shouldReturnFalseIfTheSchemaTypeIsNotSupported() {
        ParquetSourceProtoSchemaStreamType.ParquetSourceProtoSchemaStreamTypeBuilder parquetSourceProtoSchemaStreamTypeBuilder = new ParquetSourceProtoSchemaStreamType.ParquetSourceProtoSchemaStreamTypeBuilder(streamConfig, configuration, stencilClientOrchestrator);
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(PARQUET, BOUNDED)});
        when(streamConfig.getDataType()).thenReturn("JSON");

        boolean canBuild = parquetSourceProtoSchemaStreamTypeBuilder.canBuild();

        assertFalse(canBuild);
    }

    @Test
    public void shouldReturnTrueIfTheStreamTypeCanBeBuiltFromConfigs() {
        ParquetSourceProtoSchemaStreamType.ParquetSourceProtoSchemaStreamTypeBuilder parquetSourceProtoSchemaStreamTypeBuilder = new ParquetSourceProtoSchemaStreamType.ParquetSourceProtoSchemaStreamTypeBuilder(streamConfig, configuration, stencilClientOrchestrator);
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(PARQUET, BOUNDED)});
        when(streamConfig.getDataType()).thenReturn("PROTO");

        boolean canBuild = parquetSourceProtoSchemaStreamTypeBuilder.canBuild();

        assertTrue(canBuild);
    }

    @Test
    public void shouldBuildAStreamTypeWithParquetSourceAndProtoSchemaType() {
        ParquetSourceProtoSchemaStreamType.ParquetSourceProtoSchemaStreamTypeBuilder parquetSourceProtoSchemaStreamTypeBuilder = new ParquetSourceProtoSchemaStreamType.ParquetSourceProtoSchemaStreamTypeBuilder(streamConfig, configuration, stencilClientOrchestrator);

        when(streamConfig.getSchemaTable()).thenReturn("test-table");
        when(streamConfig.getEventTimestampFieldIndex()).thenReturn("5");
        when(streamConfig.getProtoClass()).thenReturn("com.tests.TestMessage");
        when(streamConfig.getParquetFilesReadOrderStrategy()).thenReturn(EARLIEST_TIME_URL_FIRST);
        when(streamConfig.getParquetFilePaths()).thenReturn(new String[]{"gs://something", "gs://anything"});
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(stencilClient.get("com.tests.TestMessage")).thenReturn(TestBookingLogMessage.getDescriptor());

        StreamType<Row> streamType = parquetSourceProtoSchemaStreamTypeBuilder.build();

        assertTrue(streamType.getSource() instanceof FileSource);
        assertEquals("test-table", streamType.getStreamName());
        assertTrue(streamType.getDeserializer() instanceof SimpleGroupDeserializer);
        assertEquals(PROTO, streamType.getInputDataType());
    }

    @Test
    public void shouldThrowDaggerConfigurationExceptionIfAnyNonSupportedSplitAssignmentStrategyIsConfigured() {
        ParquetSourceProtoSchemaStreamType.ParquetSourceProtoSchemaStreamTypeBuilder parquetSourceProtoSchemaStreamTypeBuilder = new ParquetSourceProtoSchemaStreamType.ParquetSourceProtoSchemaStreamTypeBuilder(streamConfig, configuration, stencilClientOrchestrator);

        when(streamConfig.getSchemaTable()).thenReturn("test-table");
        when(streamConfig.getEventTimestampFieldIndex()).thenReturn("5");
        when(streamConfig.getProtoClass()).thenReturn("com.tests.TestMessage");
        when(streamConfig.getParquetFilesReadOrderStrategy()).thenReturn(EARLIEST_INDEX_FIRST);
        when(streamConfig.getParquetFilePaths()).thenReturn(new String[]{"gs://something", "gs://anything"});
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(stencilClient.get("com.tests.TestMessage")).thenReturn(TestBookingLogMessage.getDescriptor());

        assertThrows(DaggerConfigurationException.class, parquetSourceProtoSchemaStreamTypeBuilder::build);
    }
}