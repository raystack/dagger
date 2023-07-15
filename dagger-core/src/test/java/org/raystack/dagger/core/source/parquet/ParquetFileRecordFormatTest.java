package org.raystack.dagger.core.source.parquet;


import org.raystack.dagger.core.metrics.reporters.statsd.SerializedStatsDReporterSupplier;
import org.raystack.dagger.core.source.parquet.reader.ParquetReader;
import org.raystack.dagger.core.source.parquet.reader.ReaderProvider;
import org.raystack.depot.metrics.StatsDReporter;
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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

public class ParquetFileRecordFormatTest {

    @Mock
    private ParquetReader parquetReader;

    @Mock
    private TypeInformation<Row> typeInformation;

    @Mock
    private Configuration configuration;

    @Mock
    private StatsDReporter statsDReporter;

    private final ReaderProvider readerProviderMock = (filePath) -> parquetReader;
    private final Supplier<TypeInformation<Row>> typeInformationProviderMock = () -> typeInformation;
    private final SerializedStatsDReporterSupplier statsDReporterSupplierMock = () -> statsDReporter;

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
    public void shouldThrowIllegalArgumentExceptionAndReportErrorWhenReaderProviderIsNotConfigured() {
        ParquetFileRecordFormat.Builder builder = ParquetFileRecordFormat.Builder.getInstance();

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> builder
                        .setTypeInformationProvider(typeInformationProviderMock)
                        .setStatsDReporterSupplier(statsDReporterSupplierMock)
                        .build());

        assertEquals("ReaderProvider is required but is set as null", ex.getMessage());
        verify(statsDReporter, times(1))
                .captureCount("fatal.exception", 1L, "fatal_exception_type=" + IllegalArgumentException.class.getName());
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionAndReportErrorWhenTypeInformationProviderIsNotConfigured() {
        ParquetFileRecordFormat.Builder builder = ParquetFileRecordFormat.Builder.getInstance();
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> builder
                        .setParquetFileReaderProvider(readerProviderMock)
                        .setStatsDReporterSupplier(statsDReporterSupplierMock)
                        .build());

        assertEquals("TypeInformationProvider is required but is set as null", ex.getMessage());
        verify(statsDReporter, times(1))
                .captureCount("fatal.exception", 1L, "fatal_exception_type=" + IllegalArgumentException.class.getName());
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionWhenStatsDReporterSupplierIsNotConfigured() {
        ParquetFileRecordFormat.Builder builder = ParquetFileRecordFormat.Builder.getInstance();
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> builder
                        .setParquetFileReaderProvider(readerProviderMock)
                        .setTypeInformationProvider(typeInformationProviderMock)
                        .build());

        assertEquals("SerializedStatsDReporterSupplier is required but is set as null", ex.getMessage());
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
    public void shouldThrowUnsupportedOperationExceptionAndReportErrorWhenRestoreReaderIsCalled() {
        ParquetFileRecordFormat.Builder builder = ParquetFileRecordFormat.Builder.getInstance();
        ParquetFileRecordFormat parquetFileRecordFormat = builder.setTypeInformationProvider(typeInformationProviderMock)
                .setParquetFileReaderProvider(readerProviderMock)
                .setStatsDReporterSupplier(statsDReporterSupplierMock)
                .build();

        UnsupportedOperationException ex = assertThrows(UnsupportedOperationException.class,
                () -> parquetFileRecordFormat.restoreReader(configuration, new Path("gs://some-path"), 12, 0, 1024));

        assertEquals("Error: ParquetReader do not have offsets and hence cannot be restored via this method.", ex.getMessage());
        verify(statsDReporter, times(1))
                .captureCount("fatal.exception", 1L, "fatal_exception_type=" + UnsupportedOperationException.class.getName());
    }
}
