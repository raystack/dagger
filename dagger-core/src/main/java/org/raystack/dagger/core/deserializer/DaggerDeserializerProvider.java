package org.raystack.dagger.core.deserializer;

import org.raystack.dagger.common.serde.DaggerDeserializer;

public interface DaggerDeserializerProvider<D> {
    DaggerDeserializer<D> getDaggerDeserializer();
    boolean canProvide();
}
