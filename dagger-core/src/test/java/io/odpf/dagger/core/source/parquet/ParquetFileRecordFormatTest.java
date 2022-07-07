package io.odpf.dagger.core.source.parquet;


import io.odpf.dagger.core.metrics.reporters.statsd.SerializedStatsDReporterSupplier;
import io.odpf.dagger.core.source.parquet.reader.ParquetReader;
import io.odpf.dagger.core.source.parquet.reader.ReaderProvider;
import io.odpf.depot.metrics.StatsDReporter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.FileRecordFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.function.Supplier;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.MockitoAnnotations.initMocks;

public class ParquetFileRecordFormatTest {

    @Mock
    private ParquetReader parquetReader;

    @Mock
    private TypeInformation<Row> typeInformation;

    @Mock
    private Configuration configuration;

    private final ReaderProvider readerProviderMock = (filePath) -> parquetReader;
    private final Supplier<TypeInformation<Row>> typeInformationProviderMock = () -> typeInformation;
    private final SerializedStatsDReporterSupplier statsDReporterSupplierMock = () -> mock(StatsDReporter.class);

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldBuildAFileRecordFormatAsPerConfiguredParameters() {
        ParquetFileRecordFormat.Builder builder = ParquetFileRecordFormat.Builder.getInstance();
        ParquetFileRecordFormat parquetFileRecordFormat = builder.setParquetFileReaderProvider(readerProviderMock)
                .setTypeInformationProvider(typeInformationProviderMock)
                .setStatsDReporterSupplier(statsDReporterSupplierMock)
                .build();

        FileRecordFormat.Reader<Row> expectedReader = parquetFileRecordFormat.createReader(configuration, new Path("gs://file-path"), 0, 1024);
        TypeInformation<Row> expectedTypeInformation = parquetFileRecordFormat.getProducedType();

        assertEquals(expectedReader, parquetReader);
        assertEquals(expectedTypeInformation, typeInformation);
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionWhenReaderProviderIsNotConfigured() {
        ParquetFileRecordFormat.Builder builder = ParquetFileRecordFormat.Builder.getInstance();

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> builder
                        .setTypeInformationProvider(typeInformationProviderMock)
                        .setStatsDReporterSupplier(statsDReporterSupplierMock)
                        .build());

        assertEquals("ReaderProvider is required but is set as null", ex.getMessage());
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionWhenTypeInformationProviderIsNotConfigured() {
        ParquetFileRecordFormat.Builder builder = ParquetFileRecordFormat.Builder.getInstance();
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> builder
                        .setParquetFileReaderProvider(readerProviderMock)
                        .setStatsDReporterSupplier(statsDReporterSupplierMock)
                        .build());

        assertEquals("TypeInformationProvider is required but is set as null", ex.getMessage());
    }

    @Test
    public void shouldReturnFalseWhenIsSplittableIsCalled() {
        ParquetFileRecordFormat.Builder builder = ParquetFileRecordFormat.Builder.getInstance();
        ParquetFileRecordFormat parquetFileRecordFormat = builder.setParquetFileReaderProvider(readerProviderMock)
                .setTypeInformationProvider(typeInformationProviderMock)
                .setStatsDReporterSupplier(statsDReporterSupplierMock)
                .build();

        assertFalse(parquetFileRecordFormat.isSplittable());
    }

    @Test
    public void shouldThrowUnsupportedOperationExceptionWhenRestoreReaderIsCalled() {
        ParquetFileRecordFormat.Builder builder = ParquetFileRecordFormat.Builder.getInstance();
        ParquetFileRecordFormat parquetFileRecordFormat = builder.setTypeInformationProvider(typeInformationProviderMock)
                .setParquetFileReaderProvider(readerProviderMock)
                .setStatsDReporterSupplier(statsDReporterSupplierMock)
                .build();

        UnsupportedOperationException ex = assertThrows(UnsupportedOperationException.class,
                () -> parquetFileRecordFormat.restoreReader(configuration, new Path("gs://some-path"), 12, 0, 1024));

        assertEquals("Error: ParquetReader do not have offsets and hence cannot be restored via this method.", ex.getMessage());
    }
}
