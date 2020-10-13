package com.gojek.daggers.postProcessors.common;

import com.gojek.dagger.common.StreamInfo;
import com.gojek.daggers.postProcessors.PostProcessorConfig;

public interface PostProcessor {
    StreamInfo process(StreamInfo streamInfo);

    boolean canProcess(PostProcessorConfig postProcessorConfig);
}
