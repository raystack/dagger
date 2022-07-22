package io.odpf.dagger.core.deserializer;

import io.odpf.dagger.common.serde.DaggerDeserializer;

public interface DaggerDeserializerProvider<D> {
    DaggerDeserializer<D> getDaggerDeserializer();
    boolean canProvide();
}
