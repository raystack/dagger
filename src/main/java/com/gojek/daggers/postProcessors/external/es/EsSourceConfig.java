package com.gojek.daggers.postProcessors.external.es;

import com.gojek.daggers.postProcessors.external.common.OutputMapping;
import com.gojek.daggers.postProcessors.external.common.SourceConfig;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EsSourceConfig implements Serializable, SourceConfig {
    private String host;
    private String port;
    private String endpointPattern;
    private String endpointVariables;
    @SerializedName(value = "type",  alternate = { "Type", "TYPE" })
    private String type;
    private String capacity;
    private String retryTimeout;
    private String socketTimeout;
    private String streamTimeout;
    private String connectTimeout;
    private boolean failOnErrors;
    private Map<String, OutputMapping> outputMapping;
    @SerializedName(value = "metricId",  alternate = { "MetricId", "METRICID" })
    private String metricId;


    public EsSourceConfig(String host, String port, String endpointPattern, String endpointVariables,
                          String type, String capacity, String connectTimeout, String retryTimeout, String socketTimeout, String streamTimeout,
                          boolean failOnErrors, Map<String, OutputMapping> outputMapping, String metricId) {
        this.host = host;
        this.port = port;
        this.endpointPattern = endpointPattern;
        this.endpointVariables = endpointVariables;
        this.type = type;
        this.capacity = capacity;
        this.connectTimeout = connectTimeout;
        this.retryTimeout = retryTimeout;
        this.socketTimeout = socketTimeout;
        this.streamTimeout = streamTimeout;
        this.failOnErrors = failOnErrors;
        this.outputMapping = outputMapping;
        this.metricId = metricId;
    }


    public String getHost() {
        return host;
    }

    public Integer getPort() {
        return Integer.valueOf(port);
    }

    @Override
    public String getPattern() {
        return endpointPattern;
    }

    @Override
    public String getVariables() {
        return endpointVariables;
    }

    @Override
    public boolean isFailOnErrors() {
        return failOnErrors;
    }

    @Override
    public String getMetricId() {
        return metricId;
    }

    @Override
    public String getType() {
        return type;
    }

    public boolean hasType() {
        return StringUtils.isNotEmpty(type);
    }

    public Integer getCapacity() {
        return Integer.valueOf(capacity);
    }

    public Integer getRetryTimeout() {
        return Integer.valueOf(retryTimeout);
    }

    public Integer getSocketTimeout() {
        return Integer.valueOf(socketTimeout);
    }

    public Integer getStreamTimeout() {
        return Integer.valueOf(streamTimeout);
    }

    public Integer getConnectTimeout() {
        return Integer.valueOf(connectTimeout);
    }

    public String getPath(String outputColumn) {
        return outputMapping.get(outputColumn).getPath();
    }

    @Override
    public HashMap<String, Object> getMandatoryFields() {
        HashMap<String, Object> mandatoryFields = new HashMap<>();
        mandatoryFields.put("host", host);
        mandatoryFields.put("port", port);
        mandatoryFields.put("endpoint_pattern", endpointPattern);
        mandatoryFields.put("type", type);
        mandatoryFields.put("capacity", capacity);
        mandatoryFields.put("connect_timeout", connectTimeout);
        mandatoryFields.put("retry_timeout", retryTimeout);
        mandatoryFields.put("socket_timeout", socketTimeout);
        mandatoryFields.put("stream_timeout", streamTimeout);
        mandatoryFields.put("fail_on_errors", failOnErrors);
        mandatoryFields.put("outputMapping", outputMapping);

        return mandatoryFields;
    }

    @Override
    public List<String> getOutputColumns() {
        return new ArrayList<>(outputMapping.keySet());
    }

}
