package com.gotocompany.dagger.core.source.parquet;

import com.gotocompany.dagger.common.configuration.Configuration;
import com.gotocompany.dagger.core.metrics.reporters.statsd.SerializedStatsDReporterSupplier;
import com.gotocompany.dagger.core.source.config.models.SourceType;
import com.gotocompany.dagger.core.source.parquet.path.HourDatePathParser;
import com.gotocompany.dagger.core.source.parquet.splitassigner.ChronologyOrderedSplitAssigner;
import com.gotocompany.depot.metrics.StatsDReporter;
import org.apache.flink.connector.file.src.assigners.LocalityAwareSplitAssigner;
import org.apache.flink.connector.file.src.reader.FileRecordFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static java.util.Collections.emptyList;
import static org.junit.Assert.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

public class ParquetFileSourceTest {
    @Mock
    private Configuration configuration;

    @Mock
    private FileRecordFormat<Row> fileRecordFormat;

    @Mock
    private StatsDReporter statsDReporter;

    private final SerializedStatsDReporterSupplier statsDReporterSupplierMock = () -> statsDReporter;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldBuildParquetFileSourceAsPerArguments() {
        ParquetFileSource.Builder builder = ParquetFileSource.Builder.getInstance();
        Path[] filePaths = new Path[]{new Path("gs://aadadc"), new Path("gs://sjsjhd")};
        ChronologyOrderedSplitAssigner.ChronologyOrderedSplitAssignerBuilder splitAssignerBuilder = new ChronologyOrderedSplitAssigner.ChronologyOrderedSplitAssignerBuilder();
        splitAssignerBuilder
                .addStatsDReporterSupplier(statsDReporterSupplierMock)
                .addPathParser(new HourDatePathParser());
        ParquetFileSource parquetFileSource = builder.setConfiguration(configuration)
                .setFileRecordFormat(fileRecordFormat)
                .setSourceType(SourceType.BOUNDED)
                .setFileSplitAssigner(splitAssignerBuilder::build)
                .setStatsDReporterSupplier(statsDReporterSupplierMock)
                .setFilePaths(filePaths)
                .build();

        assertTrue(parquetFileSource.getFileSplitAssigner().create(emptyList()) instanceof ChronologyOrderedSplitAssigner);
        assertArrayEquals(filePaths, parquetFileSource.getFilePaths());
        assertEquals(fileRecordFormat, parquetFileSource.getFileRecordFormat());
        assertEquals(configuration, parquetFileSource.getConfiguration());
        Assert.assertEquals(SourceType.BOUNDED, parquetFileSource.getSourceType());
    }

    @Test
    public void shouldThrowExceptionAndReportErrorIfSourceTypeConfiguredAsUnbounded() {
        ParquetFileSource.Builder builder = ParquetFileSource.Builder.getInstance();
        Path[] filePaths = new Path[]{new Path("gs://aadadc"), new Path("gs://sjsjhd")};

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> builder.setConfiguration(configuration)
                        .setFileRecordFormat(fileRecordFormat)
                        .setSourceType(SourceType.UNBOUNDED)
                        .setStatsDReporterSupplier(statsDReporterSupplierMock)
                        .setFileSplitAssigner(new ChronologyOrderedSplitAssigner.ChronologyOrderedSplitAssignerBuilder()::build)
                        .setFilePaths(filePaths)
                        .build());

        assertEquals("Running Parquet FileSource in UNBOUNDED mode is not supported yet", ex.getMessage());
        verify(statsDReporter, times(1))
                .captureCount("fatal.exception", 1L, "fatal_exception_type=" + IllegalArgumentException.class.getName());
    }

    @Test
    public void shouldThrowExceptionAndReportErrorIfFileRecordFormatIsNotSet() {
        ParquetFileSource.Builder builder = ParquetFileSource.Builder.getInstance();
        Path[] filePaths = new Path[]{new Path("gs://aadadc"), new Path("gs://sjsjhd")};

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> builder.setConfiguration(configuration)
                        .setSourceType(SourceType.UNBOUNDED)
                        .setStatsDReporterSupplier(statsDReporterSupplierMock)
                        .setFileSplitAssigner(new ChronologyOrderedSplitAssigner.ChronologyOrderedSplitAssignerBuilder()::build)
                        .setFilePaths(filePaths)
                        .build());

        assertEquals("FileRecordFormat is required but is set as null", ex.getMessage());
        verify(statsDReporter, times(1))
                .captureCount("fatal.exception", 1L, "fatal_exception_type=" + IllegalArgumentException.class.getName());
    }

    @Test
    public void shouldThrowExceptionAndReportErrorIfNoFilePathsSet() {
        ParquetFileSource.Builder builder = ParquetFileSource.Builder.getInstance();

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> builder.setConfiguration(configuration)
                        .setFileRecordFormat(fileRecordFormat)
                        .setSourceType(SourceType.BOUNDED)
                        .setStatsDReporterSupplier(statsDReporterSupplierMock)
                        .setFileSplitAssigner(new ChronologyOrderedSplitAssigner.ChronologyOrderedSplitAssignerBuilder()::build)
                        .build());

        assertEquals("At least one file path is required but none are provided", ex.getMessage());
        verify(statsDReporter, times(1))
                .captureCount("fatal.exception", 1L, "fatal_exception_type=" + IllegalArgumentException.class.getName());
    }

    @Test
    public void shouldThrowExceptionIfStatsDReporterSupplierIsNotSet() {
        ParquetFileSource.Builder builder = ParquetFileSource.Builder.getInstance();
        Path[] filePaths = new Path[]{new Path("gs://aadadc"), new Path("gs://sjsjhd")};

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> builder.setConfiguration(configuration)
                        .setFileRecordFormat(fileRecordFormat)
                        .setSourceType(SourceType.BOUNDED)
                        .setFileSplitAssigner(new ChronologyOrderedSplitAssigner.ChronologyOrderedSplitAssignerBuilder()::build)
                        .setFilePaths(filePaths)
                        .build());

        assertEquals("SerializedStatsDReporterSupplier is required but is set as null", ex.getMessage());
    }

    @Test
    public void shouldUseDefaultValueForSomeFieldsWhichAreNotConfiguredExplicitly() {
        ParquetFileSource.Builder builder = ParquetFileSource.Builder.getInstance();
        Path[] filePaths = new Path[]{new Path("gs://aadadc"), new Path("gs://sjsjhd")};
        ParquetFileSource parquetFileSource = builder.setConfiguration(configuration)
                .setFileRecordFormat(fileRecordFormat)
                .setStatsDReporterSupplier(statsDReporterSupplierMock)
                .setFilePaths(filePaths)
                .build();

        assertTrue(parquetFileSource.getFileSplitAssigner().create(emptyList()) instanceof LocalityAwareSplitAssigner);
        Assert.assertEquals(SourceType.BOUNDED, parquetFileSource.getSourceType());
    }

    /* this test just verifies that the code for generating the FileSource compiles successfully and runs. */
    /* Since static methods of FileSource have been used and its member properties are not exposed, it's difficult to test the */
    /* returned blackbox object */
    @Test
    public void shouldReturnAFileSourceMadeFromParquetFileSource() {
        ParquetFileSource.Builder builder = ParquetFileSource.Builder.getInstance();
        Path[] filePaths = new Path[]{new Path("gs://aadadc"), new Path("gs://sjsjhd")};
        ParquetFileSource parquetFileSource = builder.setConfiguration(configuration)
                .setFileRecordFormat(fileRecordFormat)
                .setSourceType(SourceType.BOUNDED)
                .setFileSplitAssigner(new ChronologyOrderedSplitAssigner.ChronologyOrderedSplitAssignerBuilder()::build)
                .setStatsDReporterSupplier(statsDReporterSupplierMock)
                .setFilePaths(filePaths)
                .build();

        parquetFileSource.buildFileSource();
    }
}
