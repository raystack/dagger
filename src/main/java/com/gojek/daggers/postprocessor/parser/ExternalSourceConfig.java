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

import static com.gojek.daggers.Constants.EXTERNAL_SOURCE_KEY;

public class ExternalSourceConfig {
    private List<HttpExternalSourceConfig> httpExternalSourceConfig;

    public Set<String> getExternalSourceKeys() {
        return externalSourceConfigMap.keySet();
    }

    private Map<String, Object> externalSourceConfigMap;

    public List<HttpExternalSourceConfig> getHttpExternalSourceConfig() {
        return httpExternalSourceConfig;
    }

    public static ExternalSourceConfig parse(String configuration) {
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig();
        try {
            externalSourceConfig.externalSourceConfigMap = new Gson().fromJson(configuration, Map.class);
        } catch (JsonSyntaxException exception) {
            throw new InvalidJsonException("Invalid JSON Given for " + EXTERNAL_SOURCE_KEY);
        }
        Gson gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();

        for (String key : externalSourceConfig.externalSourceConfigMap.keySet()) {
            if (key.equals("http")) {
                Type typeToken = new TypeToken<List<HttpExternalSourceConfig>>() {
                }.getType();
                externalSourceConfig.httpExternalSourceConfig = gson.fromJson(gson.toJson(externalSourceConfig.externalSourceConfigMap.get(key)), typeToken);
            } else {
                throw new IllegalArgumentException("Invalid config type");
            }
        }
        return externalSourceConfig;
    }

    public List<String> getColumns() {
        ArrayList<String> columnNames = new ArrayList<>();
        httpExternalSourceConfig.forEach(s -> {
            columnNames.addAll(s.getColumns());
        });
        return columnNames;
    }
}
