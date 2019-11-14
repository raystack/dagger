package com.gojek.daggers.postProcessors;

import com.gojek.daggers.postProcessors.external.ExternalSourceConfig;
import com.gojek.daggers.postProcessors.internal.InternalSourceConfig;
import com.gojek.daggers.postProcessors.transfromers.TransformConfig;
import com.google.common.reflect.TypeToken;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.jayway.jsonpath.InvalidJsonException;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import static com.gojek.daggers.utils.Constants.POST_PROCESSOR_CONFIG_KEY;

public class PostProcessorConfig implements Serializable {

    private ExternalSourceConfig externalSource;
    private List<TransformConfig> transformers;
    private List<InternalSourceConfig> internalSource;

    public PostProcessorConfig(ExternalSourceConfig externalSource, List<TransformConfig> transformers, List<InternalSourceConfig> internalSource) {
        this.externalSource = externalSource;
        this.transformers = transformers;
        this.internalSource = internalSource;
    }

    public static PostProcessorConfig parse(String configuration) {
        PostProcessorConfig postProcessorConfig;
        Gson gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();
        try {
            Type typeToken = new TypeToken<PostProcessorConfig>() {
            }.getType();
            postProcessorConfig = gson.fromJson(configuration, typeToken);
        } catch (JsonSyntaxException exception) {
            throw new InvalidJsonException("Invalid JSON Given for " + POST_PROCESSOR_CONFIG_KEY);
        }

        return postProcessorConfig;
    }

    public ExternalSourceConfig getExternalSource() {
        return externalSource;
    }

    public List<InternalSourceConfig> getInternalSource() {
        return internalSource;
    }

    public boolean hasExternalSource() {
        return externalSource != null && !externalSource.isEmpty();
    }

    public boolean hasInternalSource() {
        return internalSource != null;
    }

    public boolean isEmpty() {
        return !hasTransformConfigs() && !hasExternalSource() && !hasInternalSource();
    }

    public List<TransformConfig> getTransformers() {
        return transformers;
    }

    public boolean hasTransformConfigs() {
        return transformers != null && !transformers.isEmpty();
    }

    public List<String> getOutputColumnNames() {
        ArrayList<String> outputColumnNames = new ArrayList<>();
        if (externalSource != null && !externalSource.isEmpty())
            outputColumnNames.addAll(externalSource.getOutputColumnNames());
        if (internalSource != null && !internalSource.isEmpty())
            internalSource.forEach(config -> outputColumnNames.add(config.getOutputField()));
        return outputColumnNames;
    }
}
