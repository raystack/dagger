package com.gotocompany.dagger.core.processors.external.grpc;

import com.gotocompany.dagger.core.processors.common.OutputMapping;

import java.util.Map;

public class GrpcSourceConfigBuilder {
    private String endpoint;
    private int servicePort;
    private String grpcRequestProtoSchema;
    private String grpcResponseProtoSchema;
    private String grpcMethodUrl;
    private String grpcArgKeepaliveTimeMs;
    private String grpcArgKeepaliveTimeOutMs;
    private String requestPattern;
    private String requestVariables;
    private Map<String, OutputMapping> outputMapping;
    private String streamTimeout;
    private String connectTimeout;
    private boolean failOnErrors;
    private String grpcStencilUrl;
    private String type;
    private boolean retainResponseType;
    private Map<String, String> headers;
    private String metricId;
    private int capacity;

    public GrpcSourceConfigBuilder setEndpoint(String endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    public GrpcSourceConfigBuilder setServicePort(int servicePort) {
        this.servicePort = servicePort;
        return this;
    }

    public GrpcSourceConfigBuilder setGrpcRequestProtoSchema(String grpcRequestProtoSchema) {
        this.grpcRequestProtoSchema = grpcRequestProtoSchema;
        return this;
    }

    public GrpcSourceConfigBuilder setGrpcResponseProtoSchema(String grpcResponseProtoSchema) {
        this.grpcResponseProtoSchema = grpcResponseProtoSchema;
        return this;
    }

    public GrpcSourceConfigBuilder setGrpcMethodUrl(String grpcMethodUrl) {
        this.grpcMethodUrl = grpcMethodUrl;
        return this;
    }

    public GrpcSourceConfigBuilder setRequestPattern(String requestPattern) {
        this.requestPattern = requestPattern;
        return this;
    }

    public GrpcSourceConfigBuilder setRequestVariables(String requestVariables) {
        this.requestVariables = requestVariables;
        return this;
    }

    public GrpcSourceConfigBuilder setOutputMapping(Map<String, OutputMapping> outputMapping) {
        this.outputMapping = outputMapping;
        return this;
    }

    public GrpcSourceConfigBuilder setStreamTimeout(String streamTimeout) {
        this.streamTimeout = streamTimeout;
        return this;
    }

    public GrpcSourceConfigBuilder setConnectTimeout(String connectTimeout) {
        this.connectTimeout = connectTimeout;
        return this;
    }

    public GrpcSourceConfigBuilder setFailOnErrors(boolean failOnErrors) {
        this.failOnErrors = failOnErrors;
        return this;
    }

    public GrpcSourceConfigBuilder setGrpcStencilUrl(String grpcStencilUrl) {
        this.grpcStencilUrl = grpcStencilUrl;
        return this;
    }

    public GrpcSourceConfigBuilder setType(String type) {
        this.type = type;
        return this;
    }

    public GrpcSourceConfigBuilder setRetainResponseType(boolean retainResponseType) {
        this.retainResponseType = retainResponseType;
        return this;
    }

    public GrpcSourceConfigBuilder setHeaders(Map<String, String> headers) {
        this.headers = headers;
        return this;
    }

    public GrpcSourceConfigBuilder setMetricId(String metricId) {
        this.metricId = metricId;
        return this;
    }

    public GrpcSourceConfigBuilder setCapacity(int capacity) {
        this.capacity = capacity;
        return this;
    }

    public GrpcSourceConfigBuilder setGrpcArgKeepaliveTimeMs(String grpcArgKeepaliveTimeMs) {
        this.grpcArgKeepaliveTimeMs = grpcArgKeepaliveTimeMs;
        return this;
    }

    public GrpcSourceConfigBuilder setGrpcArgKeepaliveTimeOutMs(String grpcArgKeepaliveTimeOutMs) {
        this.grpcArgKeepaliveTimeOutMs = grpcArgKeepaliveTimeOutMs;
        return this;
    }

    public GrpcSourceConfig createGrpcSourceConfig() {
        return new GrpcSourceConfig(endpoint, servicePort, grpcRequestProtoSchema, grpcResponseProtoSchema, grpcMethodUrl, grpcArgKeepaliveTimeMs, grpcArgKeepaliveTimeOutMs, requestPattern, requestVariables,
                streamTimeout, connectTimeout, failOnErrors, grpcStencilUrl, type, retainResponseType, headers, outputMapping, metricId, capacity);
    }
}
