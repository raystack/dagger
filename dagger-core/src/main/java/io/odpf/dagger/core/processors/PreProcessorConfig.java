package io.odpf.dagger.core.processors;

import io.odpf.dagger.core.processors.transformers.TableTransformConfig;

import java.util.List;

public class PreProcessorConfig {
    public List<TableTransformConfig> getTableTransformers() {
        return tableTransformers;
    }

    protected List<TableTransformConfig> tableTransformers;


    public boolean hasTransformConfigs() {
        return tableTransformers != null && !tableTransformers.isEmpty();
    }

    public boolean isEmpty() {
        return !hasTransformConfigs();
    }
}
