package com.gojek.daggers.postprocessors.common;

import com.gojek.dagger.common.StreamInfo;
import com.gojek.daggers.postprocessors.PostProcessorConfig;

public interface PostProcessor {
    StreamInfo process(StreamInfo streamInfo);

    boolean canProcess(PostProcessorConfig postProcessorConfig);
}
