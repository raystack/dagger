package com.gotocompany.dagger.core.processors.external.grpc;

import com.gotocompany.dagger.core.processors.common.OutputMapping;
import com.gotocompany.dagger.core.processors.types.SourceConfig;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A class that holds Grpc configuration.
 */
public class GrpcSourceConfig implements Serializable, SourceConfig {
    private String endpoint;
    private int servicePort;
    private String grpcRequestProtoSchema;
    private String grpcResponseProtoSchema;
    private String grpcMethodUrl;
    private String requestPattern;
    private String requestVariables;
    private String grpcArgKeepaliveTimeMs;
    private String grpcArgKeepaliveTimeoutMs;
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

    /**
     * Instantiates a new Grpc source config.
     *
     * @param endpoint                the endpoint
     * @param servicePort             the service port
     * @param grpcRequestProtoSchema  the grpc request proto schema
     * @param grpcResponseProtoSchema the grpc response proto schema
     * @param grpcMethodUrl           the grpc method url
     * @param requestPattern          the request pattern
     * @param requestVariables        the request variables
     * @param outputMapping           the output mapping
     */
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

    /**
     * Instantiates a new Grpc source config with specified grpc stencil url.
     *
     * @param endpoint                          the endpoint
     * @param servicePort                       the service port
     * @param grpcRequestProtoSchema            the grpc request proto schema
     * @param grpcResponseProtoSchema           the grpc response proto schema
     * @param grpcMethodUrl                     the grpc method url
     * @param requestPattern                    the request pattern
     * @param grpcArgKeepaliveTimeMs            the grpc Keepalive Time ms
     * @param grpcArgKeepaliveTimeoutMs         the grpc Keepalive Timeout ms
     * @param requestVariables                  the request variables
     * @param streamTimeout                     the stream timeout
     * @param connectTimeout                    the connect timeout
     * @param failOnErrors                      the fail on errors
     * @param grpcStencilUrl                    the grpc stencil url
     * @param type                              the type
     * @param retainResponseType                the retain response type
     * @param headers                           the headers
     * @param outputMapping                     the output mapping
     * @param metricId                          the metric id
     * @param capacity                          the capacity
     */
    public GrpcSourceConfig(String endpoint, int servicePort, String grpcRequestProtoSchema, String grpcResponseProtoSchema, String grpcMethodUrl, String grpcArgKeepaliveTimeMs, String grpcArgKeepaliveTimeoutMs , String requestPattern, String requestVariables, String streamTimeout, String connectTimeout, boolean failOnErrors, String grpcStencilUrl, String type, boolean retainResponseType, Map<String, String> headers, Map<String, OutputMapping> outputMapping, String metricId, int capacity) {
        this.endpoint = endpoint;
        this.servicePort = servicePort;
        this.grpcRequestProtoSchema = grpcRequestProtoSchema;
        this.grpcResponseProtoSchema = grpcResponseProtoSchema;
        this.grpcMethodUrl = grpcMethodUrl;
        this.grpcArgKeepaliveTimeMs = grpcArgKeepaliveTimeMs;
        this.grpcArgKeepaliveTimeoutMs = grpcArgKeepaliveTimeoutMs;
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

    /**
     * Gets connect timeout.
     *
     * @return the connect timeout
     */
    public Integer getConnectTimeout() {
        return Integer.parseInt(connectTimeout);
    }

    /**
     * Gets endpoint.
     *
     * @return the endpoint
     */
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

    /**
     * Gets stream timeout.
     *
     * @return the stream timeout
     */
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

    /**
     * Gets headers.
     *
     * @return the headers
     */
    public Map<String, String> getHeaders() {
        return headers;
    }

    /**
     * Gets output mapping.
     *
     * @return the output mapping
     */
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

    /**
     * Gets grpc response proto schema.
     *
     * @return the grpc response proto schema
     */
    public String getGrpcResponseProtoSchema() {
        return grpcResponseProtoSchema;
    }

    /**
     * Gets grpc method url.
     *
     * @return the grpc method url
     */
    public String getGrpcMethodUrl() {
        return grpcMethodUrl;
    }

    /**
     * Gets grpc arg keepalive time ms.
     *
     * @return grpc arg keepalive time ms
     */
    public String getGrpcArgKeepaliveTimeMs() {
        return grpcArgKeepaliveTimeMs;
    }

    /**
     * Gets grpc arg keepalive timeout ms.
     *
     * @return grpc arg keepalive timeout ms
     */
    public String getGrpcArgKeepaliveTimeoutMs() {
        return grpcArgKeepaliveTimeoutMs;
    }

    /**
     * Sets grpc arg keepalive time ms.
     *
     * @param grpcArgKeepaliveTimeMs the grpc arg keepalive time ms
     */
    public void setGrpcArgKeepaliveTimeMs(String grpcArgKeepaliveTimeMs) {
        this.grpcArgKeepaliveTimeMs = grpcArgKeepaliveTimeMs;
    }

    /**
     * Sets grpc arg keepalive timeout ms.
     *
     * @param grpcArgKeepaliveTimeOutMs the grpc arg keepalive timeout ms
     */
    public void setGrpcArgKeepaliveTimeOutMs(String grpcArgKeepaliveTimeOutMs) {
        this.grpcArgKeepaliveTimeoutMs = grpcArgKeepaliveTimeOutMs;
    }

    /**
     * Gets service port.
     *
     * @return the service port
     */
    public int getServicePort() {
        return servicePort;
    }

    /**
     * Check if type config is not empty.
     *
     * @return the boolean
     */
    public boolean hasType() {
        return StringUtils.isNotEmpty(type);
    }

    /**
     * Check if it is retain response type.
     *
     * @return the boolean
     */
    public boolean isRetainResponseType() {
        return retainResponseType;
    }

    /**
     * Sets fail on errors.
     *
     * @param failOnErrors the fail on errors
     */
    public void setFailOnErrors(boolean failOnErrors) {
        this.failOnErrors = failOnErrors;
    }

    /**
     * Sets type.
     *
     * @param type the type
     */
    public void setType(String type) {
        this.type = type;
    }

    /**
     * Sets retain response type.
     *
     * @param retainResponseType the retain response type
     */
    public void setRetainResponseType(boolean retainResponseType) {
        this.retainResponseType = retainResponseType;
    }

    /**
     * Sets headers.
     *
     * @param headers the headers
     */
    public void setHeaders(Map<String, String> headers) {
        this.headers = headers;
    }

    /**
     * Gets grpc request proto schema.
     *
     * @return the grpc request proto schema
     */
    public String getGrpcRequestProtoSchema() {
        return grpcRequestProtoSchema;
    }

    /**
     * Gets capacity.
     *
     * @return the capacity
     */
    public int getCapacity() {
        return capacity;
    }

    /**
     * Gets grpc stencil url.
     *
     * @return the grpc stencil url
     */
    public List<String> getGrpcStencilUrl() {
        return Arrays.stream(grpcStencilUrl.split(","))
                .map(String::trim)
                .collect(Collectors.toList());
    }
}
