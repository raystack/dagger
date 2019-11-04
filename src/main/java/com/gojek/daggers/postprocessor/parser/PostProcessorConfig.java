package com.gojek.daggers.postprocessor.parser;

import com.gojek.daggers.postprocessor.configs.ExternalSourceConfig;
import com.google.common.reflect.TypeToken;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.jayway.jsonpath.InvalidJsonException;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

import static com.gojek.daggers.Constants.POST_PROCESSOR_CONFIG_KEY;

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

    public static PostProcessorConfig parse(String configuration){
        PostProcessorConfig postProcessorConfig;
        Gson gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();
        try {
            Type typeToken = new TypeToken<PostProcessorConfig>() {
            }.getType();
            postProcessorConfig = gson.fromJson(configuration, typeToken);
        } catch (JsonSyntaxException exception) {
            throw new InvalidJsonException("Invalid JSON Given for " + POST_PROCESSOR_CONFIG_KEY);
        }

        if (postProcessorConfig.hasExternalSource()) {
            if(postProcessorConfig.externalSource.isEmpty())
                throw new IllegalArgumentException("Invalid config type");
        }
        return postProcessorConfig;
    }

    public List<String> getColumns(){
        return externalSource.getColumnNames();
    }
}
