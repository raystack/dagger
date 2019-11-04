package com.gojek.daggers.postprocessor.parser;

import com.gojek.daggers.postprocessor.configs.ExternalSourceConfig;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class PostProcessorConfig implements Serializable {

    private ExternalSourceConfig externalSource;
    private List<TransformConfig> transformers;

    public PostProcessorConfig(ExternalSourceConfig externalSource, List<TransformConfig> transformers) {
        this.externalSource = externalSource;
        this.transformers = transformers;
    }

    public ExternalSourceConfig getExternalSource() {
        return externalSource;
    }

    public boolean hasExternalSource() {
        return externalSource != null;
    }

    public List<TransformConfig> getTransformers() {
        return transformers;
    }

    public boolean hasTransformConfigs() {
        return transformers != null;
    }
}
