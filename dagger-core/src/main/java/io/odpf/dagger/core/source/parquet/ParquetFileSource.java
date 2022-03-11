package io.odpf.dagger.core.source.parquet;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.core.source.SourceType;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.connector.file.src.reader.FileRecordFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;

import static io.odpf.dagger.core.source.SourceType.UNBOUNDED;

public class ParquetFileSource {
    private final SourceType sourceType;
    private final Path[] filePaths;
    private final Configuration configuration;
    private final FileRecordFormat<Row> fileRecordFormat;
    private final FileSplitAssigner.Provider fileSplitAssigner;


    public ParquetFileSource(SourceType sourceType,
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
            throw new IllegalConfigurationException("Running KafkaSource in UNBOUNDED mode is not supported yet");
        }
        return FileSource.forRecordFileFormat(fileRecordFormat, filePaths)
                .setSplitAssigner(fileSplitAssigner)
                .build();
    }
}
