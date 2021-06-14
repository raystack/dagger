package io.odpf.dagger.core.processors;

import io.odpf.dagger.core.processors.transformers.TableTransformConfig;

import java.util.List;

/**
 * The Preprocessor config.
 */
public class PreProcessorConfig {
    /**
     * Gets table transformers.
     *
     * @return the table transformers
     */
    public List<TableTransformConfig> getTableTransformers() {
        return tableTransformers;
    }

    /**
     * The Table transformers.
     */
    protected List<TableTransformConfig> tableTransformers;

    /**
     * Check if it has transformer configs.
     *
     * @return the boolean
     */
    public boolean hasTransformConfigs() {
        return tableTransformers != null && !tableTransformers.isEmpty();
    }

    /**
     * Check if transformers config is empty.
     *
     * @return the boolean
     */
    public boolean isEmpty() {
        return !hasTransformConfigs();
    }
}
