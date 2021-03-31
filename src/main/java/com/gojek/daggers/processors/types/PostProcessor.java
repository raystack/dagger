package com.gojek.daggers.processors.types;

import com.gojek.daggers.core.StreamInfo;
import com.gojek.daggers.processors.PostProcessorConfig;

public interface PostProcessor {

    StreamInfo process(StreamInfo streamInfo);

    boolean canProcess(PostProcessorConfig processorConfig);
}
