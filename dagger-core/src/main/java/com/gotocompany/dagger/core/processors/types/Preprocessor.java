package com.gotocompany.dagger.core.processors.types;

import com.gotocompany.dagger.common.core.StreamInfo;
import com.gotocompany.dagger.core.processors.PreProcessorConfig;

/**
 * The interface Preprocessor.
 */
public interface Preprocessor {
    /**
     * Process stream info.
     *
     * @param streamInfo the stream info
     * @return the stream info
     */
    StreamInfo process(StreamInfo streamInfo);

    /**
     * Check if it can process the preprocessor with given config.
     *
     * @param processorConfig the processor config
     * @return the boolean
     */
    boolean canProcess(PreProcessorConfig processorConfig);
}
