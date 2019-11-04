package com.gojek.daggers.postprocessor.parser;

import com.gojek.daggers.postprocessor.configs.ExternalSourceConfig;
import com.gojek.daggers.postprocessor.configs.HttpExternalSourceConfig;
import com.google.common.reflect.TypeToken;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.jayway.jsonpath.InvalidJsonException;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.gojek.daggers.Constants.POST_PROCESSOR_CONFIG_KEY;

public class PostProcessorConfigHandler {

    private PostProcessorConfig postProcessorConfig;

    public PostProcessorConfigHandler(PostProcessorConfig postProcessorConfig) {
        this.postProcessorConfig = postProcessorConfig;
    }

    public ExternalSourceConfig getExternalSourceConfig() {
        return postProcessorConfig.getExternalSource();
    }

    public List<HttpExternalSourceConfig> getHttpExternalSourceConfig() {
        return postProcessorConfig.getExternalSource().getHttpConfig();
    }

    public List<TransformConfig> getTransformConfig() {
        return postProcessorConfig.getTransformers();
    }

    public static PostProcessorConfigHandler parse(String configuration){
        return new PostProcessorConfigHandler(PostProcessorConfig.parse(configuration));
    }

    public List<String> getColumns() {
        return postProcessorConfig.getColumns();
    }
}
