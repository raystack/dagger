package io.odpf.dagger.core.source.parquet;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.core.source.SourceType;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.connector.file.src.assigners.LocalityAwareSplitAssigner;
import org.apache.flink.connector.file.src.reader.FileRecordFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;

import static com.google.api.client.util.Preconditions.checkArgument;

public class ParquetFileSourceBuilder {
    private SourceType sourceType;
    private Path[] filePaths;
    private FileRecordFormat<Row> fileRecordFormat;
    private Configuration configuration;
    private FileSplitAssigner.Provider fileSplitAssigner;

    public static ParquetFileSourceBuilder getInstance() {
        return new ParquetFileSourceBuilder();
    }

    private ParquetFileSourceBuilder() {
        this.sourceType = SourceType.BOUNDED;
        this.configuration = null;
        this.fileRecordFormat = null;
        this.filePaths = new Path[0];
        this.fileSplitAssigner = LocalityAwareSplitAssigner::new;
    }

    public ParquetFileSourceBuilder setSourceType(SourceType sourceType) {
        this.sourceType = sourceType;
        return this;
    }

    public ParquetFileSourceBuilder setFileRecordFormat(FileRecordFormat<Row> fileRecordFormat) {
        this.fileRecordFormat = fileRecordFormat;
        return this;
    }

    public ParquetFileSourceBuilder setFileSplitAssigner(FileSplitAssigner.Provider fileSplitAssigner) {
        this.fileSplitAssigner = fileSplitAssigner;
        return this;
    }

    public ParquetFileSourceBuilder setFilePaths(Path[] filePaths) {
        this.filePaths = filePaths;
        return this;
    }

    public ParquetFileSourceBuilder setConfiguration(Configuration configuration) {
        this.configuration = configuration;
        return this;
    }

    /* other validations if required before creating the file source can be put here */
    private void sanityCheck() {
        checkArgument(fileRecordFormat != null, "FileRecordFormat is required but is set as null");
        checkArgument(filePaths.length!=0, "ReaderProvider is required but is set as null");
    }

    public ParquetFileSource build() {
        sanityCheck();
        return new ParquetFileSource(sourceType,
                configuration,
                fileRecordFormat,
                filePaths,
                fileSplitAssigner);
    }
}