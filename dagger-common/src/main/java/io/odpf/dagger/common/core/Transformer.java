package io.odpf.dagger.common.core;

/**
 * The interface for all the transformer.
 */
public interface Transformer {
    StreamInfo transform(StreamInfo streamInfo);

    /**
     * Each created {@link DaggerContext} instance
     */
    static DaggerContext daggerContextGenerator() {
        return DaggerContext.getInstance();
    }
}
