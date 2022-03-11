package io.odpf.dagger.core.source.parquet.reader;

import org.apache.flink.connector.file.src.reader.FileRecordFormat;
import org.apache.flink.types.Row;

@FunctionalInterface
public interface ReaderProvider {
    FileRecordFormat.Reader<Row> getReader(String filePath);
}
