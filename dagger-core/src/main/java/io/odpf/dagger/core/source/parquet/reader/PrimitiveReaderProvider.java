package io.odpf.dagger.core.source.parquet.reader;

import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.serde.DaggerDeserializer;
import io.odpf.dagger.core.source.StreamConfig;
import org.apache.flink.types.Row;

public class PrimitiveReaderProvider implements ReaderProvider {

    private final StreamConfig streamConfig;
    private final StencilClientOrchestrator stencilClientOrchestrator;
    private final DaggerDeserializer<Row> daggerDeserializer;

    public PrimitiveReaderProvider(StreamConfig streamConfig,
                                   StencilClientOrchestrator stencilClientOrchestrator,
                                   DaggerDeserializer<Row> daggerDeserializer) {

        this.streamConfig = streamConfig;
        this.stencilClientOrchestrator = stencilClientOrchestrator;
        this.daggerDeserializer = daggerDeserializer;
    }

    @Override
    public PrimitiveReader getReader() {
        return new PrimitiveReader(streamConfig, stencilClientOrchestrator, daggerDeserializer);
    }
}