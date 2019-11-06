package com.gojek.daggers.postProcessors.common;

import com.gojek.daggers.core.StreamInfo;
import com.gojek.daggers.postProcessors.PostProcessorConfig;
import org.apache.flink.configuration.Configuration;

public interface PostProcessor {
    StreamInfo process(StreamInfo streamInfo);

    boolean canProcess(PostProcessorConfig postProcessorConfig);
}
