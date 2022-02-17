package io.odpf.dagger.core.source.parquet;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.serde.DaggerDeserializer;
import io.odpf.dagger.core.source.SourceType;
import io.odpf.dagger.core.source.StreamConfig;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.assigners.LocalityAwareSplitAssigner;
import org.apache.flink.connector.file.src.reader.FileRecordFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;

import java.util.Arrays;

public class ParquetFileSourceFactory {
    public static FileSource<Row> getFileSource(SourceType sourceType,
                                                StreamConfig streamConfig,
                                                Configuration configuration,
                                                DaggerDeserializer<Row> deserializer) {
        Path[] filePaths = Arrays.stream(streamConfig.getParquetFilePaths())
                .map(Path::new)
                .toArray(Path[]::new);
        ParquetFileSourceBuilder parquetFileSourceBuilder = new ParquetFileSourceBuilder();

        return parquetFileSourceBuilder.setFilePaths(filePaths)
                .setConfiguration(configuration)
                .setFileRecordFormat(null)
                .setSourceType(sourceType)
                .setFileSplitAssigner(LocalityAwareSplitAssigner::new)
                .build();
    }

    /* TODO */
    private static FileRecordFormat<Row> getParquetFileRecordFormat(StreamConfig streamConfig, Configuration configuration) {
        return null;
    }
}