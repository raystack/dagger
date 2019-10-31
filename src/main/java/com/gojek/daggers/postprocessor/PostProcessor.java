package com.gojek.daggers.postprocessor;

import com.gojek.daggers.StreamInfo;
import com.gojek.daggers.postprocessor.parser.PostProcessorConfig;
import org.apache.flink.configuration.Configuration;

public interface PostProcessor {
    StreamInfo process(StreamInfo streamInfo);
    boolean canProcess(Configuration configuration, PostProcessorConfig postProcessorConfig);
}
