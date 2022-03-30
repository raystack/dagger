package io.odpf.dagger.core.source.parquet;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.core.source.SourceType;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.connector.file.src.assigners.LocalityAwareSplitAssigner;
import org.apache.flink.connector.file.src.reader.FileRecordFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;

import static com.google.api.client.util.Preconditions.checkArgument;
import static io.odpf.dagger.core.source.SourceType.UNBOUNDED;

public class ParquetFileSource {
    private final SourceType sourceType;
    private final Path[] filePaths;
    private final Configuration configuration;
    private final FileRecordFormat<Row> fileRecordFormat;
    private final FileSplitAssigner.Provider fileSplitAssigner;


    private ParquetFileSource(SourceType sourceType,
                             Configuration configuration,
                             FileRecordFormat<Row> fileRecordFormat,
                             Path[] filePaths,
                             FileSplitAssigner.Provider fileSplitAssigner) {
        this.sourceType = sourceType;
        this.configuration = configuration;
        this.filePaths = filePaths;
        this.fileRecordFormat = fileRecordFormat;
        this.fileSplitAssigner = fileSplitAssigner;
    }

    public FileSource<Row> getFileSource() {
        if (sourceType == UNBOUNDED) {
            throw new IllegalConfigurationException("Running Parquet FileSource in UNBOUNDED mode is not supported yet");
        }
        return FileSource.forRecordFileFormat(fileRecordFormat, filePaths)
                .setSplitAssigner(fileSplitAssigner)
                .build();
    }

    public static class Builder {
        private SourceType sourceType;
        private Path[] filePaths;
        private FileRecordFormat<Row> fileRecordFormat;
        private Configuration configuration;
        private FileSplitAssigner.Provider fileSplitAssigner;

        public static Builder getInstance() {
            return new Builder();
        }

        private Builder() {
            this.sourceType = SourceType.BOUNDED;
            this.configuration = null;
            this.fileRecordFormat = null;
            this.filePaths = new Path[0];
            this.fileSplitAssigner = LocalityAwareSplitAssigner::new;
        }

        public Builder setSourceType(SourceType sourceType) {
            this.sourceType = sourceType;
            return this;
        }

        public Builder setFileRecordFormat(FileRecordFormat<Row> fileRecordFormat) {
            this.fileRecordFormat = fileRecordFormat;
            return this;
        }

        public Builder setFileSplitAssigner(FileSplitAssigner.Provider fileSplitAssigner) {
            this.fileSplitAssigner = fileSplitAssigner;
            return this;
        }

        public Builder setFilePaths(Path[] filePaths) {
            this.filePaths = filePaths;
            return this;
        }

        public Builder setConfiguration(Configuration configuration) {
            this.configuration = configuration;
            return this;
        }

        /* other validations if required before creating the file source can be put here */
        /* for example, checking that all the file paths conform to just one partitioning strategy */
        private void sanityCheck() {
            checkArgument(fileRecordFormat != null, "FileRecordFormat is required but is set as null");
            checkArgument(filePaths.length != 0, "At least one file path is required but is not provided");
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
}
