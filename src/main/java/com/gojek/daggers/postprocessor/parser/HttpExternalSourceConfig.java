package com.gojek.daggers.postprocessor.parser;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HttpExternalSourceConfig implements Serializable, Validator {
    private String endpoint;
    private String verb;
    private String bodyColumnFromSql;
    private String streamTimeout;
    private String connectTimeout;
    private boolean failOnErrors;
    private Map<String, String> headers;
    private Map<String, OutputMapping> outputMapping;

    public HttpExternalSourceConfig(String endpoint, String verb, String bodyColumnFromSql, String streamTimeout, String connectTimeout, boolean failOnErrors, Map<String, String> headers, Map<String, OutputMapping> outputMapping) {
        this.endpoint = endpoint;
        this.verb = verb;
        this.bodyColumnFromSql = bodyColumnFromSql;
        this.streamTimeout = streamTimeout;
        this.connectTimeout = connectTimeout;
        this.failOnErrors = failOnErrors;
        this.headers = headers;
        this.outputMapping = outputMapping;
    }

    public String getConnectTimeout() {
        return connectTimeout;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getVerb() {
        return verb;
    }

    public String getBodyColumnFromSql() {
        return bodyColumnFromSql;
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

    public HashMap<String, Object> getMandatoryFields() {
        HashMap<String, Object> mandatoryFields = new HashMap<>();
        mandatoryFields.put("endpoint", endpoint);
        mandatoryFields.put("bodyColumnFromSql", bodyColumnFromSql);
        mandatoryFields.put("streamTimeout", streamTimeout);
        mandatoryFields.put("connectTimeout", connectTimeout);
        mandatoryFields.put("outputMapping", outputMapping);

        return mandatoryFields;
    }
}
