package io.odpf.dagger.core.processors.types;

import io.odpf.dagger.common.core.StreamInfo;
import io.odpf.dagger.core.processors.PostProcessorConfig;

public interface PostProcessor {

    StreamInfo process(StreamInfo streamInfo);

    boolean canProcess(PostProcessorConfig processorConfig);
}
