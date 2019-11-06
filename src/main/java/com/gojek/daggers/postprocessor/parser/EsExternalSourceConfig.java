package com.gojek.daggers.postprocessor.parser;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class EsExternalSourceConfig implements Validator, Serializable {
    private String host;
    private String query_param_pattern;
    private String query_param_variables;
    private String type;
    private String retry_timeout;
    private String socket_timeout;
    private String stream_timeout;
    private Map<String, OutputMapping> outputMapping;

    EsExternalSourceConfig(String host, String query_param_pattern, String query_param_variables,
                           String type, String retry_timeout, String socket_timeout, String stream_timeout,
                           Map<String, OutputMapping> outputMapping) {
        this.host = host;
        this.query_param_pattern = query_param_pattern;
        this.query_param_variables = query_param_variables;
        this.type = type;
        this.retry_timeout = retry_timeout;
        this.socket_timeout = socket_timeout;
        this.stream_timeout = stream_timeout;
        this.outputMapping = outputMapping;
    }


    public String getHost() {
        return host;
    }

    public String getQuery_param_pattern() {
        return query_param_pattern;
    }

    public String getQuery_param_variables() {
        return query_param_variables;
    }

    public String getType() {
        return type;
    }

    public String getRetry_timeout() {
        return retry_timeout;
    }

    public String getSocket_timeout() {
        return socket_timeout;
    }

    public String getStream_timeout() {
        return stream_timeout;
    }

    public Map<String, OutputMapping> getOutputMapping() {
        return outputMapping;
    }

    @Override
    public HashMap<String, Object> getMandatoryFields() {
        HashMap<String, Object> mandatoryFields = new HashMap<>();
        mandatoryFields.put("host", host);
        mandatoryFields.put("query_param_pattern", query_param_pattern);
        mandatoryFields.put("query_param_variables", query_param_variables);
        mandatoryFields.put("type", type);
        mandatoryFields.put("outputMapping", outputMapping);

        return mandatoryFields;
    }
}
