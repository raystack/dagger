package io.odpf.dagger.core.processors.external.es;

import io.odpf.dagger.core.processors.common.OutputMapping;

import java.util.Map;

public class EsSourceConfigBuilder {
    private String host;
    private String port;
    private String user;
    private String password;
    private String endpointPattern;
    private String endpointVariables;
    private String type;
    private String capacity;
    private String connectTimeout;
    private String retryTimeout;
    private String socketTimeout;
    private String streamTimeout;
    private boolean failOnErrors;
    private Map<String, OutputMapping> outputMapping;
    private String metricId;
    private boolean retainResponseType;

    public EsSourceConfigBuilder setHost(String host) {
        this.host = host;
        return this;
    }

    public EsSourceConfigBuilder setPort(String port) {
        this.port = port;
        return this;
    }

    public EsSourceConfigBuilder setUser(String user) {
        this.user = user;
        return this;
    }

    public EsSourceConfigBuilder setPassword(String password) {
        this.password = password;
        return this;
    }

    public EsSourceConfigBuilder setEndpointPattern(String endpointPattern) {
        this.endpointPattern = endpointPattern;
        return this;
    }

    public EsSourceConfigBuilder setEndpointVariables(String endpointVariables) {
        this.endpointVariables = endpointVariables;
        return this;
    }

    public EsSourceConfigBuilder setType(String type) {
        this.type = type;
        return this;
    }

    public EsSourceConfigBuilder setCapacity(String capacity) {
        this.capacity = capacity;
        return this;
    }

    public EsSourceConfigBuilder setConnectTimeout(String connectTimeout) {
        this.connectTimeout = connectTimeout;
        return this;
    }

    public EsSourceConfigBuilder setRetryTimeout(String retryTimeout) {
        this.retryTimeout = retryTimeout;
        return this;
    }

    public EsSourceConfigBuilder setSocketTimeout(String socketTimeout) {
        this.socketTimeout = socketTimeout;
        return this;
    }

    public EsSourceConfigBuilder setStreamTimeout(String streamTimeout) {
        this.streamTimeout = streamTimeout;
        return this;
    }

    public EsSourceConfigBuilder setFailOnErrors(boolean failOnErrors) {
        this.failOnErrors = failOnErrors;
        return this;
    }

    public EsSourceConfigBuilder setOutputMapping(Map<String, OutputMapping> outputMapping) {
        this.outputMapping = outputMapping;
        return this;
    }

    public EsSourceConfigBuilder setMetricId(String metricId) {
        this.metricId = metricId;
        return this;
    }

    public EsSourceConfigBuilder setRetainResponseType(boolean retainResponseType) {
        this.retainResponseType = retainResponseType;
        return this;
    }

    public EsSourceConfig createEsSourceConfig() {
        return new EsSourceConfig(host, port, user, password, endpointPattern, endpointVariables, type, capacity, connectTimeout, retryTimeout, socketTimeout, streamTimeout, failOnErrors, outputMapping, metricId, retainResponseType);
    }
}
