package com.gojek.daggers.processors.types;

import com.gojek.daggers.core.StreamInfo;
import com.gojek.daggers.processors.PreProcessorConfig;

public interface Preprocessor {
    StreamInfo process(StreamInfo streamInfo);

    boolean canProcess(PreProcessorConfig processorConfig);
}
