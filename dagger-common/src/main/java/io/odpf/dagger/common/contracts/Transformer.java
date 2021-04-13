package io.odpf.dagger.common.contracts;

import io.odpf.dagger.common.core.StreamInfo;

public interface Transformer {
    StreamInfo transform(StreamInfo streamInfo);
}

