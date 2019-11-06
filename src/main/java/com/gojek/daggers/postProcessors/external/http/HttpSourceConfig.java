package com.gojek.daggers.postProcessors.external.http;

import com.gojek.daggers.postProcessors.external.common.OutputMapping;
import com.gojek.daggers.postProcessors.external.common.SourceConfig;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HttpSourceConfig implements Serializable, SourceConfig {
    private String endpoint;
    private String verb;
    private String bodyPattern;
    private String bodyVariables;
    private String streamTimeout;
    private String connectTimeout;
    private boolean failOnErrors;
    private String type;
    private String capacity;
    private Map<String, String> headers;
    private Map<String, OutputMapping> outputMapping;

    public HttpSourceConfig(String endpoint, String verb, String bodyPattern, String bodyVariables, String streamTimeout, String connectTimeout, boolean failOnErrors, String type, String capacity, Map<String, String> headers, Map<String, OutputMapping> outputMapping) {
        this.endpoint = endpoint;
        this.verb = verb;
        this.bodyPattern = bodyPattern;
        this.bodyVariables = bodyVariables;
        this.streamTimeout = streamTimeout;
        this.connectTimeout = connectTimeout;
        this.failOnErrors = failOnErrors;
        this.type = type;
        this.capacity = capacity;
        this.headers = headers;
        this.outputMapping = outputMapping;
    }

    public Integer getConnectTimeout() {
        return Integer.parseInt(connectTimeout);
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getVerb() {
        return verb;
    }

    public String getBodyVariables() {
        return bodyVariables;
    }

    public String getBodyPattern() {
        return bodyPattern;
    }

    public Integer getStreamTimeout() {
        return Integer.valueOf(streamTimeout);
    }

    public boolean isFailOnErrors() {
        return failOnErrors;
    }

    public String getType() {
        return type;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public Map<String, OutputMapping> getOutputMapping() {
        return outputMapping;
    }

    @Override
    public List<String> getOutputColumns() {
        return new ArrayList<>(outputMapping.keySet());
    }

    public HashMap<String, Object> getMandatoryFields() {
        HashMap<String, Object> mandatoryFields = new HashMap<>();
        mandatoryFields.put("endpoint", endpoint);
        mandatoryFields.put("verb", verb);
        mandatoryFields.put("failOnErrors", failOnErrors);
        mandatoryFields.put("capacity", capacity);
        mandatoryFields.put("bodyPattern", bodyPattern);
        mandatoryFields.put("bodyVariables", bodyVariables);
        mandatoryFields.put("streamTimeout", streamTimeout);
        mandatoryFields.put("connectTimeout", connectTimeout);
        mandatoryFields.put("outputMapping", outputMapping);

        return mandatoryFields;
    }

    public Integer getCapacity() {
        return Integer.parseInt(capacity);
    }
}
