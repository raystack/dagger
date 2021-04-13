package io.odpf.dagger.processors.types;

import io.odpf.dagger.common.core.StreamInfo;
import io.odpf.dagger.processors.PostProcessorConfig;

public interface PostProcessor {

    StreamInfo process(StreamInfo streamInfo);

    boolean canProcess(PostProcessorConfig processorConfig);
}
