package io.odpf.dagger.core.source.parquet.reader;

import org.apache.flink.connector.file.src.reader.FileRecordFormat;
import org.apache.flink.types.Row;

import java.io.Serializable;

public interface ReaderProvider extends Serializable {
    FileRecordFormat.Reader<Row> getReader(String filePath);
    FileRecordFormat.Reader<Row> getRestoredReader(String filePath, long restoredOffset, long splitOffset);
}
