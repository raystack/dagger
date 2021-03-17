package com.gojek.daggers.processors.types;

import com.gojek.dagger.common.StreamInfo;
import com.gojek.daggers.processors.PostProcessorConfig;

public interface PostProcessor {

    StreamInfo process(StreamInfo streamInfo);

    boolean canProcess(PostProcessorConfig processorConfig);
}
