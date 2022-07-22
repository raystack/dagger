package io.odpf.dagger.core.source.parquet.reader;


import io.odpf.dagger.common.exceptions.serde.DaggerDeserializationException;
import io.odpf.dagger.core.metrics.reporters.statsd.SerializedStatsDReporterSupplier;
import io.odpf.dagger.common.serde.parquet.deserialization.SimpleGroupDeserializer;
import io.odpf.dagger.core.exception.ParquetFileSourceReaderInitializationException;
import io.odpf.depot.metrics.StatsDReporter;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.types.Row;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static io.odpf.dagger.core.metrics.aspects.ParquetReaderAspects.READER_CLOSED;
import static io.odpf.dagger.core.metrics.aspects.ParquetReaderAspects.READER_CREATED;
import static io.odpf.dagger.core.metrics.aspects.ParquetReaderAspects.READER_ROWS_EMITTED;
import static io.odpf.dagger.core.metrics.aspects.ParquetReaderAspects.READER_ROW_DESERIALIZATION_TIME;
import static io.odpf.dagger.core.metrics.aspects.ParquetReaderAspects.READER_ROW_READ_TIME;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Types.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.mockito.internal.verification.VerificationModeFactory.times;

public class ParquetReaderTest {
    @Mock
    private SimpleGroupDeserializer deserializer;

    @Rule
    public TemporaryFolder tempFolder = TemporaryFolder.builder().assureDeletion().build();

    @Mock
    private StatsDReporter statsDReporter;

    private final SerializedStatsDReporterSupplier statsDReporterSupplierMock = () -> statsDReporter;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldRaiseMetricsWhenInitialized() {
        ParquetReader.ParquetReaderProvider provider = new ParquetReader.ParquetReaderProvider(deserializer, statsDReporterSupplierMock);
        ClassLoader classLoader = getClass().getClassLoader();
        String filePath = classLoader.getResource("test_file.parquet").getPath();

        provider.getReader(filePath);

        verify(statsDReporter, Mockito.times(1)).captureCount(READER_CREATED.getValue(), 1L, "component=parquet_reader");
    }

    @Test
    public void shouldRaiseMetricsWhenReadingFileAndDeserializingToRows() throws IOException {
        ParquetReader.ParquetReaderProvider provider = new ParquetReader.ParquetReaderProvider(deserializer, statsDReporterSupplierMock);
        ClassLoader classLoader = getClass().getClassLoader();
        String filePath = classLoader.getResource("test_file.parquet").getPath();

        ParquetReader reader = provider.getReader(filePath);
        reader.read();

        verify(statsDReporter, Mockito.times(1)).captureCount(READER_ROWS_EMITTED.getValue(), 1L, "component=parquet_reader");

        ArgumentCaptor<String> measurementNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Long> executionTimeCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<String> tagCaptor = ArgumentCaptor.forClass(String.class);

        verify(statsDReporter, Mockito.times(2)).captureHistogram(measurementNameCaptor.capture(), executionTimeCaptor.capture(), tagCaptor.capture());

        assertEquals(READER_ROW_READ_TIME.getValue(), measurementNameCaptor.getAllValues().get(0));
        assertEquals(READER_ROW_DESERIALIZATION_TIME.getValue(), measurementNameCaptor.getAllValues().get(1));
        assertEquals("component=parquet_reader", tagCaptor.getAllValues().get(0));
        assertEquals("component=parquet_reader", tagCaptor.getAllValues().get(1));
    }

    @Test
    public void shouldRaiseMetricsWhenClosingTheReader() throws IOException {
        ParquetReader.ParquetReaderProvider provider = new ParquetReader.ParquetReaderProvider(deserializer, statsDReporterSupplierMock);
        ClassLoader classLoader = getClass().getClassLoader();
        String filePath = classLoader.getResource("test_file.parquet").getPath();

        provider.getReader(filePath).close();

        verify(statsDReporter, Mockito.times(1)).captureCount(READER_CLOSED.getValue(), 1L, "component=parquet_reader");
    }

    @Test
    public void shouldReportErrorAndRethrowWhenDaggerDeserializerThrowsException() {
        when(deserializer.deserialize(any(SimpleGroup.class))).thenThrow(DaggerDeserializationException.class);

        ParquetReader.ParquetReaderProvider provider = new ParquetReader.ParquetReaderProvider(deserializer, statsDReporterSupplierMock);
        ClassLoader classLoader = getClass().getClassLoader();
        String filePath = classLoader.getResource("test_file.parquet").getPath();

        assertThrows(DaggerDeserializationException.class, () -> provider.getReader(filePath).read());

        verify(statsDReporter, Mockito.times(1))
                .captureCount("fatal.exception", 1L, "fatal_exception_type=" + DaggerDeserializationException.class.getName());
    }

    @Test
    public void shouldCreateReadersConfiguredWithTheSameDeserializerButForDifferentFilePaths() throws IOException {
        when(deserializer.deserialize(any(SimpleGroup.class))).thenReturn(Row.of("same", "deserializer"));
        ParquetReader.ParquetReaderProvider provider = new ParquetReader.ParquetReaderProvider(deserializer, statsDReporterSupplierMock);
        ClassLoader classLoader = getClass().getClassLoader();

        String filePath1 = classLoader.getResource("test_file.parquet").getPath();
        ParquetReader reader1 = provider.getReader(filePath1);

        String filePath2 = classLoader.getResource("multiple_row_groups_test_file.parquet").getPath();
        ParquetReader reader2 = provider.getReader(filePath2);

        assertEquals(reader1.read(), reader2.read());
    }

    @Test
    public void shouldReadFileAndCallDeserializerWithSimpleGroupWhenReadIsCalled() throws IOException {
        ParquetReader.ParquetReaderProvider provider = new ParquetReader.ParquetReaderProvider(deserializer, statsDReporterSupplierMock);
        ClassLoader classLoader = getClass().getClassLoader();
        ParquetReader reader = provider.getReader(classLoader.getResource("test_file.parquet").getPath());

        reader.read();
        reader.read();
        reader.read();

        SimpleGroup[] allExpectedSimpleGroups = getSimpleGroups();
        ArgumentCaptor<SimpleGroup> simpleGroupCaptor = ArgumentCaptor.forClass(SimpleGroup.class);
        verify(deserializer, times(3)).deserialize(simpleGroupCaptor.capture());
        List<SimpleGroup> allActualSimpleGroups = simpleGroupCaptor.getAllValues();

        for (int i = 0; i < 3; i++) {
            SimpleGroup expectedSimpleGroup = allExpectedSimpleGroups[i];
            SimpleGroup actualSimpleGroup = allActualSimpleGroups.get(i);
            assertEquals(expectedSimpleGroup.getType(), actualSimpleGroup.getType());
            assertEquals(expectedSimpleGroup.getString("name", 0), actualSimpleGroup.getString("name", 0));
            assertEquals(expectedSimpleGroup.getLong("age", 0), actualSimpleGroup.getLong("age", 0));
            assertEquals(expectedSimpleGroup.getString("residence", 0), actualSimpleGroup.getString("residence", 0));
        }
    }

    @Test
    public void shouldBeAbleToReadParquetFileContainingMultipleRowGroups() throws IOException {
        ParquetReader.ParquetReaderProvider provider = new ParquetReader.ParquetReaderProvider(deserializer, statsDReporterSupplierMock);
        ClassLoader classLoader = getClass().getClassLoader();
        ParquetReader reader = provider.getReader(classLoader.getResource("multiple_row_groups_test_file.parquet").getPath());

        reader.read();
        reader.read();
        reader.read();
        reader.read();
        reader.read();
        reader.read();

        SimpleGroup[] allExpectedSimpleGroups = ArrayUtils.addAll(getSimpleGroups(), getSimpleGroups());
        ArgumentCaptor<SimpleGroup> simpleGroupCaptor = ArgumentCaptor.forClass(SimpleGroup.class);
        verify(deserializer, times(6)).deserialize(simpleGroupCaptor.capture());
        List<SimpleGroup> allActualSimpleGroups = simpleGroupCaptor.getAllValues();

        for (int i = 0; i < 6; i++) {
            SimpleGroup expectedSimpleGroup = allExpectedSimpleGroups[i];
            SimpleGroup actualSimpleGroup = allActualSimpleGroups.get(i);
            assertEquals(expectedSimpleGroup.getType(), actualSimpleGroup.getType());
            assertEquals(expectedSimpleGroup.getString("name", 0), actualSimpleGroup.getString("name", 0));
            assertEquals(expectedSimpleGroup.getLong("age", 0), actualSimpleGroup.getLong("age", 0));
            assertEquals(expectedSimpleGroup.getString("residence", 0), actualSimpleGroup.getString("residence", 0));
        }
    }

    @Test
    public void shouldReturnDeserializedValueWhenRecordsPresentAndNullWhenNoMoreDataLeftToRead() throws IOException {
        ParquetReader.ParquetReaderProvider provider = new ParquetReader.ParquetReaderProvider(deserializer, statsDReporterSupplierMock);
        ClassLoader classLoader = getClass().getClassLoader();
        ParquetReader reader = provider.getReader(classLoader.getResource("test_file.parquet").getPath());
        when(deserializer.deserialize(any(SimpleGroup.class))).thenReturn(Row.of("some value"));

        assertEquals(Row.of("some value"), reader.read());
        assertEquals(Row.of("some value"), reader.read());
        assertEquals(Row.of("some value"), reader.read());
        assertNull(reader.read());
    }

    @Test
    public void shouldThrowIOExceptionIfReadIsCalledAfterCallingClose() throws IOException {
        ParquetReader.ParquetReaderProvider provider = new ParquetReader.ParquetReaderProvider(deserializer, statsDReporterSupplierMock);
        ClassLoader classLoader = getClass().getClassLoader();
        ParquetReader reader = provider.getReader(classLoader.getResource("test_file.parquet").getPath());

        reader.close();

        assertThrows(IOException.class, reader::read);
    }

    @Test
    public void shouldThrowParquetFileSourceReaderInitializationExceptionAndReportErrorIfCannotConstructReaderForTheFile() throws IOException {
        final File tempFile = tempFolder.newFile("test_file.parquet");
        ParquetReader.ParquetReaderProvider provider = new ParquetReader.ParquetReaderProvider(deserializer, statsDReporterSupplierMock);

        assertThrows(ParquetFileSourceReaderInitializationException.class, () -> provider.getReader(tempFile.getPath()));
        verify(statsDReporter, times(1))
                .captureCount("fatal.exception", 1L, "fatal_exception_type=" + ParquetFileSourceReaderInitializationException.class.getName());
    }

    @Test
    public void shouldReturnCheckPointedPositionWithNoOffsetAndZeroRecordsAfterOffsetWhenReadHasNotBeenCalledYet() {
        ParquetReader.ParquetReaderProvider provider = new ParquetReader.ParquetReaderProvider(deserializer, statsDReporterSupplierMock);
        ClassLoader classLoader = getClass().getClassLoader();
        ParquetReader reader = provider.getReader(classLoader.getResource("test_file.parquet").getPath());

        CheckpointedPosition expectedCheckpointedPosition = new CheckpointedPosition(CheckpointedPosition.NO_OFFSET, 0);

        assertEquals(expectedCheckpointedPosition, reader.getCheckpointedPosition());
    }

    @Test
    public void shouldUpdateCheckPointedPositionWithNoOffsetAndCountOfTotalRecordsReadYet() throws IOException {
        ParquetReader.ParquetReaderProvider provider = new ParquetReader.ParquetReaderProvider(deserializer, statsDReporterSupplierMock);
        ClassLoader classLoader = getClass().getClassLoader();
        ParquetReader reader = provider.getReader(classLoader.getResource("test_file.parquet").getPath());

        reader.read();
        assertEquals(new CheckpointedPosition(CheckpointedPosition.NO_OFFSET, 1), reader.getCheckpointedPosition());

        reader.read();
        assertEquals(new CheckpointedPosition(CheckpointedPosition.NO_OFFSET, 2), reader.getCheckpointedPosition());
    }

    @Test
    public void shouldNotUpdateCheckPointedPositionWhenNoMoreRecordsToRead() throws IOException {
        ParquetReader.ParquetReaderProvider provider = new ParquetReader.ParquetReaderProvider(deserializer, statsDReporterSupplierMock);
        ClassLoader classLoader = getClass().getClassLoader();
        ParquetReader reader = provider.getReader(classLoader.getResource("test_file.parquet").getPath());

        reader.read();
        reader.read();
        reader.read();
        /* This 4th call to read will return null as there are only 3 rows in test_file.parquet */
        reader.read();

        CheckpointedPosition expectedCheckpointedPosition = new CheckpointedPosition(CheckpointedPosition.NO_OFFSET, 3);

        assertEquals(expectedCheckpointedPosition, reader.getCheckpointedPosition());
    }

    private SimpleGroup[] getSimpleGroups() {
        GroupType expectedSchema = buildMessage()
                .optional(BINARY).as(LogicalTypeAnnotation.stringType()).named("name")
                .optional(INT64).named("age")
                .optional(BINARY).as(LogicalTypeAnnotation.stringType()).named("residence")
                .named("schema");

        SimpleGroup group1 = new SimpleGroup(expectedSchema);
        group1.add("name", "Ajay");
        group1.add("age", 24L);
        group1.add("residence", "Mumbai");

        SimpleGroup group2 = new SimpleGroup(expectedSchema);
        group2.add("name", "Utkarsh");
        group2.add("age", 25L);
        group2.add("residence", "Delhi");

        SimpleGroup group3 = new SimpleGroup(expectedSchema);
        group3.add("name", "Samay");
        group3.add("age", 29L);
        group3.add("residence", "Pune");

        return new SimpleGroup[]{group1, group2, group3};
    }
}
