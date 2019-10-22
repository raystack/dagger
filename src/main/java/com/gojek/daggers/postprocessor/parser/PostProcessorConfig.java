package com.gojek.daggers.postprocessor.parser;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class PostProcessorConfig implements Serializable {

    private Map<String, Object> externalSource;
    private List<TransformConfig> transformers;

    public PostProcessorConfig(Map<String, Object> externalSource, List<TransformConfig> transformers) {
        this.externalSource = externalSource;
        this.transformers = transformers;
    }

    public Map<String, Object> getExternalSource() {
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
