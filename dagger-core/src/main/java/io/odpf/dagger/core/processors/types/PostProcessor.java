package io.odpf.dagger.core.processors.types;

import io.odpf.dagger.common.core.StreamInfo;
import io.odpf.dagger.core.processors.PostProcessorConfig;

/**
 * The interface Post processor.
 */
public interface PostProcessor {

    /**
     * Process stream info.
     *
     * @param streamInfo the stream info
     * @return the stream info
     */
    StreamInfo process(StreamInfo streamInfo);

    /**
     * Check if it can process the post processor with given config.
     *
     * @param processorConfig the processor config
     * @return the boolean
     */
    boolean canProcess(PostProcessorConfig processorConfig);
}
