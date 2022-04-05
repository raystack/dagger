package io.odpf.dagger.core.source.parquet;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.core.source.SourceType;
import io.odpf.dagger.core.source.parquet.ParquetFileSource.Builder;
import io.odpf.dagger.core.source.parquet.splitassigner.ChronologyOrderedSplitAssigner;
import org.apache.flink.connector.file.src.assigners.LocalityAwareSplitAssigner;
import org.apache.flink.connector.file.src.reader.FileRecordFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import static java.util.Collections.*;
import static org.junit.Assert.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class ParquetFileSourceTest {
    @Mock
    private Configuration configuration;

    @Mock
    private FileRecordFormat<Row> fileRecordFormat;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldBuildParquetFileSourceAsPerArguments() {
        Builder builder = Builder.getInstance();
        Path[] filePaths = new Path[]{new Path("gs://aadadc"), new Path("gs://sjsjhd")};
        ParquetFileSource parquetFileSource = builder.setConfiguration(configuration)
                .setFileRecordFormat(fileRecordFormat)
                .setSourceType(SourceType.BOUNDED)
                .setFileSplitAssigner(ChronologyOrderedSplitAssigner::new)
                .setFilePaths(filePaths)
                .build();

        assertTrue(parquetFileSource.getFileSplitAssigner().create(emptyList()) instanceof ChronologyOrderedSplitAssigner);
        assertArrayEquals(filePaths, parquetFileSource.getFilePaths());
        assertEquals(fileRecordFormat, parquetFileSource.getFileRecordFormat());
        assertEquals(configuration, parquetFileSource.getConfiguration());
        assertEquals(SourceType.BOUNDED, parquetFileSource.getSourceType());
    }

    @Test
    public void shouldThrowExceptionIfSourceTypeConfiguredAsUnbounded() {
        Builder builder = Builder.getInstance();
        Path[] filePaths = new Path[]{new Path("gs://aadadc"), new Path("gs://sjsjhd")};

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> builder.setConfiguration(configuration)
                        .setFileRecordFormat(fileRecordFormat)
                        .setSourceType(SourceType.UNBOUNDED)
                        .setFileSplitAssigner(ChronologyOrderedSplitAssigner::new)
                        .setFilePaths(filePaths)
                        .build());

        assertEquals("Running Parquet FileSource in UNBOUNDED mode is not supported yet", ex.getMessage());
    }

    @Test
    public void shouldThrowExceptionIfFileRecordFormatIsNotSet() {
        Builder builder = Builder.getInstance();
        Path[] filePaths = new Path[]{new Path("gs://aadadc"), new Path("gs://sjsjhd")};

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> builder.setConfiguration(configuration)
                        .setSourceType(SourceType.UNBOUNDED)
                        .setFileSplitAssigner(ChronologyOrderedSplitAssigner::new)
                        .setFilePaths(filePaths)
                        .build());

        assertEquals("FileRecordFormat is required but is set as null", ex.getMessage());
    }

    @Test
    public void shouldThrowExceptionIfNoFilePathsSet() {
        Builder builder = Builder.getInstance();

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> builder.setConfiguration(configuration)
                        .setFileRecordFormat(fileRecordFormat)
                        .setSourceType(SourceType.BOUNDED)
                        .setFileSplitAssigner(ChronologyOrderedSplitAssigner::new)
                        .build());

        assertEquals("At least one file path is required but none are provided", ex.getMessage());
    }

    @Test
    public void shouldUseDefaultValueForSomeFieldsWhichAreNotConfiguredExplicitly() {
        Builder builder = Builder.getInstance();
        Path[] filePaths = new Path[]{new Path("gs://aadadc"), new Path("gs://sjsjhd")};
        ParquetFileSource parquetFileSource = builder.setConfiguration(configuration)
                .setFileRecordFormat(fileRecordFormat)
                .setFilePaths(filePaths)
                .build();

        assertTrue(parquetFileSource.getFileSplitAssigner().create(emptyList()) instanceof LocalityAwareSplitAssigner);
        assertEquals(SourceType.BOUNDED, parquetFileSource.getSourceType());
    }

    /* this test just verifies that the code for generating the FileSource compiles successfully and runs. */
    /* Since static methods of FileSource have been used and its member properties are not exposed, it's difficult to test the */
    /* returned blackbox object */
    @Test
    public void shouldReturnAFileSourceMadeFromParquetFileSource() {
        Builder builder = Builder.getInstance();
        Path[] filePaths = new Path[]{new Path("gs://aadadc"), new Path("gs://sjsjhd")};
        ParquetFileSource parquetFileSource = builder.setConfiguration(configuration)
                .setFileRecordFormat(fileRecordFormat)
                .setSourceType(SourceType.BOUNDED)
                .setFileSplitAssigner(ChronologyOrderedSplitAssigner::new)
                .setFilePaths(filePaths)
                .build();

        parquetFileSource.buildFileSource();
    }
}
