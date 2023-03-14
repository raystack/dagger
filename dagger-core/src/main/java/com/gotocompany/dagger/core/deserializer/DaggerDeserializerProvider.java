package com.gotocompany.dagger.core.deserializer;

import com.gotocompany.dagger.common.serde.DaggerDeserializer;

public interface DaggerDeserializerProvider<D> {
    DaggerDeserializer<D> getDaggerDeserializer();
    boolean canProvide();
}
