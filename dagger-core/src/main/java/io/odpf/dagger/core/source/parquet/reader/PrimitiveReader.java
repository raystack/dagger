package io.odpf.dagger.core.source.parquet.reader;

import io.odpf.dagger.common.serde.parquet.deserialization.SimpleGroupDeserializer;
import org.apache.flink.connector.file.src.reader.FileRecordFormat;
import org.apache.flink.types.Row;
import org.apache.hadoop.fs.Path;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;

/* TODO */
public class PrimitiveReader implements FileRecordFormat.Reader<Row>, Serializable {
    Path hadoopFilePath;
    SimpleGroupDeserializer simpleGroupDeserializer;


    public PrimitiveReader(String filePath, SimpleGroupDeserializer simpleGroupDeserializer) {
        this.hadoopFilePath = new Path(filePath);
        this.simpleGroupDeserializer = simpleGroupDeserializer;
    }

    @Nullable
    @Override
    public Row read() throws IOException {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
