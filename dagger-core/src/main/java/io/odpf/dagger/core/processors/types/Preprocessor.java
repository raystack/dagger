package io.odpf.dagger.core.processors.types;

import io.odpf.dagger.common.core.StreamInfo;
import io.odpf.dagger.core.processors.PreProcessorConfig;

public interface Preprocessor {
    StreamInfo process(StreamInfo streamInfo);

    boolean canProcess(PreProcessorConfig processorConfig);
}
