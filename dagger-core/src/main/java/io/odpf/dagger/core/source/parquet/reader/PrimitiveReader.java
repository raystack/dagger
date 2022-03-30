package io.odpf.dagger.core.source.parquet.reader;

import io.odpf.dagger.common.serde.parquet.deserialization.SimpleGroupDeserializer;
import lombok.Getter;
import org.apache.flink.connector.file.src.reader.FileRecordFormat;
import org.apache.flink.types.Row;
import org.apache.hadoop.fs.Path;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;

public class PrimitiveReader implements FileRecordFormat.Reader<Row>, Serializable {
    @Getter
    private final Path hadoopFilePath;
    @Getter
    private final SimpleGroupDeserializer simpleGroupDeserializer;

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


    public static class PrimitiveReaderProvider implements ReaderProvider {
        private final SimpleGroupDeserializer simpleGroupDeserializer;

        public PrimitiveReaderProvider(SimpleGroupDeserializer simpleGroupDeserializer) {
            this.simpleGroupDeserializer = simpleGroupDeserializer;
        }

        @Override
        public PrimitiveReader getReader(String filePath) {
            return new PrimitiveReader(filePath, simpleGroupDeserializer);
        }
    }
}
