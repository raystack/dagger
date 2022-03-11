package io.odpf.dagger.core.source.parquet.reader;

import io.odpf.dagger.common.serde.parquet.deserialization.SimpleGroupDeserializer;

public class PrimitiveReaderProvider implements ReaderProvider {
    private final SimpleGroupDeserializer simpleGroupDeserializer;

    public PrimitiveReaderProvider(SimpleGroupDeserializer simpleGroupDeserializer) {
        this.simpleGroupDeserializer = simpleGroupDeserializer;
    }

    @Override
    public PrimitiveReader getReader(String filePath) {
        return new PrimitiveReader(filePath, simpleGroupDeserializer);
    }
}