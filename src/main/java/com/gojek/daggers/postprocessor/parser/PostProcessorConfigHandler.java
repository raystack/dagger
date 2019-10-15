package com.gojek.daggers.postprocessor.parser;

import com.google.common.reflect.TypeToken;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.jayway.jsonpath.InvalidJsonException;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.gojek.daggers.Constants.POST_PROCESSOR_CONFIG_KEY;

public class PostProcessorConfigHandler {

    private PostProcessorConfig postProcessorConfig;

    private Map<String, Object> externalSourceConfigMap;

    private List<HttpExternalSourceConfig> httpExternalSourceConfig;

    private List<TransformConfig> transformConfig;

    private Map postProcessorConfigMap;

    public Map<String, Object> getExternalSourceConfigMap() {
        return externalSourceConfigMap;
    }

    public Set<String> getExternalSourceKeys() {
        return externalSourceConfigMap.keySet();
    }

    public List<HttpExternalSourceConfig> getHttpExternalSourceConfig() {
        return httpExternalSourceConfig;
    }

    public List<TransformConfig> getTransformConfig() {
        return transformConfig;
    }

    public static PostProcessorConfigHandler parse(String configuration) {
        PostProcessorConfigHandler postProcessorConfigHandler = new PostProcessorConfigHandler();
        Gson gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();
        try {
            postProcessorConfigHandler.postProcessorConfigMap = new Gson().fromJson(configuration, Map.class);
            Type typeToken = new TypeToken<PostProcessorConfig>() {
            }.getType();
            postProcessorConfigHandler.postProcessorConfig = gson.fromJson(configuration, typeToken);
        } catch (JsonSyntaxException exception) {
            throw new InvalidJsonException("Invalid JSON Given for " + POST_PROCESSOR_CONFIG_KEY);
        }

        if (postProcessorConfigHandler.postProcessorConfig.getExternalSource() != null) {
            postProcessorConfigHandler.externalSourceConfigMap = postProcessorConfigHandler.postProcessorConfig.getExternalSource();
            Map<String, Object> externalSourceConfigMap = postProcessorConfigHandler.externalSourceConfigMap;

            for (String externalSourceKey : externalSourceConfigMap.keySet()) {
                if (externalSourceKey.equals("http")) {
                    Type typeToken = new TypeToken<List<HttpExternalSourceConfig>>() {
                    }.getType();
                    postProcessorConfigHandler.httpExternalSourceConfig = gson.fromJson(gson.toJson(externalSourceConfigMap.get(externalSourceKey)), typeToken);
                } else {
                    throw new IllegalArgumentException("Invalid config type");
                }
            }
        }
        if (postProcessorConfigHandler.postProcessorConfig.getTransformers() != null) {
            Type typeToken = new TypeToken<List<TransformConfig>>() {
            }.getType();
            postProcessorConfigHandler.transformConfig = gson.fromJson(gson.toJson(postProcessorConfigHandler.postProcessorConfig.getTransformers()), typeToken);
        }

        return postProcessorConfigHandler;
    }

    public List<String> getColumns() {
        ArrayList<String> columnNames = new ArrayList<>();
        httpExternalSourceConfig.forEach(s -> {
            columnNames.addAll(s.getColumns());
        });
        return columnNames;
    }
}
