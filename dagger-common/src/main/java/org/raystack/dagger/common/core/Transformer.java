package org.raystack.dagger.common.core;

/**
 * The interface for all the transformer.
 */
public interface Transformer {
    StreamInfo transform(StreamInfo streamInfo);
}
