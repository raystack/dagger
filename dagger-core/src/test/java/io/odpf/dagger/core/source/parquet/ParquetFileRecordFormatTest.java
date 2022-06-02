package io.odpf.dagger.core.source.parquet;


import io.odpf.dagger.core.source.parquet.reader.ParquetReader;
import io.odpf.dagger.core.source.parquet.reader.ReaderProvider;
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

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldBuildAFileRecordFormatAsPerConfiguredParameters() {
        ParquetFileRecordFormat.Builder builder = ParquetFileRecordFormat.Builder.getInstance();
        ParquetFileRecordFormat parquetFileRecordFormat = builder.setParquetFileReaderProvider(readerProviderMock)
                .setTypeInformationProvider(typeInformationProviderMock)
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
                        .build());

        assertEquals("ReaderProvider is required but is set as null", ex.getMessage());
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionWhenTypeInformationProviderIsNotConfigured() {
        ParquetFileRecordFormat.Builder builder = ParquetFileRecordFormat.Builder.getInstance();
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> builder
                        .setParquetFileReaderProvider(readerProviderMock)
                        .build());

        assertEquals("TypeInformationProvider is required but is set as null", ex.getMessage());
    }

    @Test
    public void shouldReturnFalseWhenIsSplittableIsCalled() {
        ParquetFileRecordFormat.Builder builder = ParquetFileRecordFormat.Builder.getInstance();
        ParquetFileRecordFormat parquetFileRecordFormat = builder.setParquetFileReaderProvider(readerProviderMock)
                .setTypeInformationProvider(typeInformationProviderMock)
                .build();

        assertFalse(parquetFileRecordFormat.isSplittable());
    }

    @Test
    public void shouldThrowUnsupportedOperationExceptionWhenRestoreReaderIsCalled() {
        ParquetFileRecordFormat.Builder builder = ParquetFileRecordFormat.Builder.getInstance();
        ParquetFileRecordFormat parquetFileRecordFormat = builder.setTypeInformationProvider(typeInformationProviderMock)
                .setParquetFileReaderProvider(readerProviderMock)
                .build();

        UnsupportedOperationException ex = assertThrows(UnsupportedOperationException.class,
                () -> parquetFileRecordFormat.restoreReader(configuration, new Path("gs://some-path"), 12, 0, 1024));

        assertEquals("Error: Restoring a reader from saved state is not implemented yet", ex.getMessage());
    }
}
