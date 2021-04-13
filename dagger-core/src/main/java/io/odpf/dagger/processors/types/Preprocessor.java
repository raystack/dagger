package io.odpf.dagger.processors.types;

import io.odpf.dagger.common.core.StreamInfo;
import io.odpf.dagger.processors.PreProcessorConfig;

public interface Preprocessor {
    StreamInfo process(StreamInfo streamInfo);

    boolean canProcess(PreProcessorConfig processorConfig);
}
