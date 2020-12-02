package com.gojek.daggers.postProcessors.external.grpc;

import com.gojek.daggers.postProcessors.external.common.OutputMapping;
import com.gojek.daggers.postProcessors.external.common.SourceConfig;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class GrpcSourceConfig implements Serializable, SourceConfig {
    private String endpoint;
    private int servicePort;
    private String grpcRequestProtoSchema;
    private String grpcResponseProtoSchema;
    private String grpcMethodUrl;
    private String requestPattern;
    private String requestVariables;
    private String streamTimeout;
    private String connectTimeout;
    private boolean failOnErrors;
    private String type;
    private boolean retainResponseType;
    private String grpcStencilUrl;
    @SerializedName(value = "headers", alternate = {"Headers", "HEADERS"})
    private Map<String, String> headers;
    private Map<String, OutputMapping> outputMapping;
    @SerializedName(value = "metricId", alternate = {"MetricId", "METRICID"})
    private String metricId;
    private int capacity;

    public GrpcSourceConfig(String endpoint, int servicePort, String grpcRequestProtoSchema, String grpcResponseProtoSchema, String grpcMethodUrl, String requestPattern, String requestVariables, Map<String, OutputMapping> outputMapping) {
        this.endpoint = endpoint;
        this.servicePort = servicePort;
        this.grpcRequestProtoSchema = grpcRequestProtoSchema;
        this.grpcResponseProtoSchema = grpcResponseProtoSchema;
        this.grpcMethodUrl = grpcMethodUrl;
        this.requestPattern = requestPattern;
        this.requestVariables = requestVariables;
        this.outputMapping = outputMapping;
    }

    public GrpcSourceConfig(String endpoint, int servicePort, String grpcRequestProtoSchema, String grpcResponseProtoSchema, String grpcMethodUrl, String requestPattern, String requestVariables, String streamTimeout, String connectTimeout, boolean failOnErrors, String grpcStencilUrl, String type, boolean retainResponseType, Map<String, String> headers, Map<String, OutputMapping> outputMapping, String metricId, int capacity) {
        this.endpoint = endpoint;
        this.servicePort = servicePort;
        this.grpcRequestProtoSchema = grpcRequestProtoSchema;
        this.grpcResponseProtoSchema = grpcResponseProtoSchema;
        this.grpcMethodUrl = grpcMethodUrl;
        this.requestPattern = requestPattern;
        this.requestVariables = requestVariables;
        this.streamTimeout = streamTimeout;
        this.connectTimeout = connectTimeout;
        this.failOnErrors = failOnErrors;
        this.grpcStencilUrl = grpcStencilUrl;
        this.type = type;
        this.retainResponseType = retainResponseType;
        this.headers = headers;
        this.outputMapping = outputMapping;
        this.metricId = metricId;
        this.capacity = capacity;
    }

    public Integer getConnectTimeout() {
        return Integer.parseInt(connectTimeout);
    }

    public String getEndpoint() {
        return endpoint;
    }

    @Override
    public String getPattern() {
        return requestPattern;
    }

    @Override
    public String getVariables() {
        return requestVariables;
    }

    public Integer getStreamTimeout() {
        return Integer.valueOf(streamTimeout);
    }

    public boolean isFailOnErrors() {
        return failOnErrors;
    }

    @Override
    public String getMetricId() {
        return metricId;
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
        mandatoryFields.put("servicePort", servicePort);
        mandatoryFields.put("grpcRequestProtoSchema", grpcRequestProtoSchema);
        mandatoryFields.put("grpcResponseProtoSchema", grpcResponseProtoSchema);
        mandatoryFields.put("grpcMethodUrl", grpcMethodUrl);
        mandatoryFields.put("failOnErrors", failOnErrors);
        mandatoryFields.put("requestPattern", requestPattern);
        mandatoryFields.put("requestVariables", requestVariables);
        mandatoryFields.put("streamTimeout", streamTimeout);
        mandatoryFields.put("connectTimeout", connectTimeout);
        mandatoryFields.put("outputMapping", outputMapping);

        return mandatoryFields;
    }

    public String getGrpcResponseProtoSchema() {
        return grpcResponseProtoSchema;
    }

    public String getGrpcMethodUrl() {
        return grpcMethodUrl;
    }

    public int getServicePort() {
        return servicePort;
    }

    public boolean hasType() {
        return StringUtils.isNotEmpty(type);
    }

    public boolean isRetainResponseType() {
        return retainResponseType;
    }

    public void setFailOnErrors(boolean failOnErrors) {
        this.failOnErrors = failOnErrors;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setRetainResponseType(boolean retainResponseType) {
        this.retainResponseType = retainResponseType;
    }

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers;
    }

    public String getGrpcRequestProtoSchema() {
        return grpcRequestProtoSchema;
    }

    public int getCapacity() {
        return capacity;
    }

    public List<String> getGrpcStencilUrl() {
        return Arrays.stream(grpcStencilUrl.split(","))
                .map(String::trim)
                .collect(Collectors.toList());
    }
}
