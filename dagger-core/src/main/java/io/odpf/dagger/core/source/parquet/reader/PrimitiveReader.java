package io.odpf.dagger.core.source.parquet.reader;

import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.serde.DaggerDeserializer;
import io.odpf.dagger.core.source.StreamConfig;
import org.apache.flink.connector.file.src.reader.FileRecordFormat;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;

/* TODO */
public class PrimitiveReader implements FileRecordFormat.Reader<Row>, Serializable {

    public PrimitiveReader(StreamConfig streamConfig,
                           StencilClientOrchestrator stencilClientOrchestrator,
                           DaggerDeserializer<Row> daggerDeserializer) {

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
