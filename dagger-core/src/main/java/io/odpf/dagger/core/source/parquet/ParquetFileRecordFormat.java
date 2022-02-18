package io.odpf.dagger.core.source.parquet;

import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.core.source.StreamConfig;
import io.odpf.dagger.core.source.parquet.reader.ReaderProvider;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.FileRecordFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;

import java.io.IOException;

/* TODO */
public class ParquetFileRecordFormat implements FileRecordFormat<Row> {
    private final StencilClientOrchestrator stencilClientOrchestrator;
    private final StreamConfig streamConfig;
    private final ReaderProvider parquetFileReaderProvider;

    public ParquetFileRecordFormat(StreamConfig streamConfig, ReaderProvider parquetFileReaderProvider, StencilClientOrchestrator stencilClientOrchestrator) {
        this.stencilClientOrchestrator = stencilClientOrchestrator;
        this.parquetFileReaderProvider = parquetFileReaderProvider;
        this.streamConfig = streamConfig;
    }

    @Override
    public Reader<Row> createReader(Configuration config, Path filePath, long splitOffset, long splitLength) throws IOException {
        return parquetFileReaderProvider.getReader();
    }

    @Override
    public Reader<Row> restoreReader(Configuration config, Path filePath, long restoredOffset, long splitOffset, long splitLength) throws IOException {
        return parquetFileReaderProvider.getReader();
    }

    @Override
    public boolean isSplittable() {
        return false;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return null;
    }
}