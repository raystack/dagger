package org.raystack.dagger.core.source.parquet.reader;

import org.apache.flink.connector.file.src.reader.FileRecordFormat;
import org.apache.flink.types.Row;

import java.io.Serializable;

@FunctionalInterface
public interface ReaderProvider extends Serializable {
    FileRecordFormat.Reader<Row> getReader(String filePath);
}
