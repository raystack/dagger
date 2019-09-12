package com.gojek.daggers.postprocessor.parser;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HttpExternalSourceConfig implements Serializable {
    private String endpoint;
    private String verb;
    private String bodyField;
    private String streamTimeout;
    private String connectTimeout;
    private boolean failOnErrors;
    private Map<String, String> headers;
    private Map<String, OutputMapping> outputMapping;

    private List<Object> mandatoryFields;
    private List<String> fieldsMissing;

    public String getConnectTimeout() {
        return connectTimeout;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getVerb() {
        return verb;
    }

    public String getBodyField() {
        return bodyField;
    }

    public String getStreamTimeout() {
        return streamTimeout;
    }

    public boolean isFailOnErrors() {
        return failOnErrors;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public Map<String, OutputMapping> getOutputMapping() {
        return outputMapping;
    }

    public List<String> getColumns() {
        return new ArrayList<>(outputMapping.keySet());
    }

    public void validate() throws IllegalArgumentException {
        HashMap<String, Object> mandatoryFields = new HashMap<>();
        mandatoryFields.put("endpoint", endpoint);
        mandatoryFields.put("bodyField", bodyField);
        mandatoryFields.put("streamTimeout", streamTimeout);
        mandatoryFields.put("connectTimeout", connectTimeout);
        mandatoryFields.put("outputMapping", outputMapping);
        fieldsMissing = new ArrayList<>();
        mandatoryFields.forEach((key, value) -> {
            if (value == null)
                fieldsMissing.add(key);
        });
        if (fieldsMissing.size() != 0)
            throw new IllegalArgumentException("Missing required fields: " + fieldsMissing.toString());
        outputMapping.forEach((key, value) -> value.validate());
    }
}
