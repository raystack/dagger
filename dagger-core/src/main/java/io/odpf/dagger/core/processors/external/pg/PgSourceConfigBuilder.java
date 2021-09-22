package io.odpf.dagger.core.processors.external.pg;

import java.util.Map;

public class PgSourceConfigBuilder {
    private String host;
    private String port;
    private String user;
    private String password;
    private String database;
    private String type;
    private String capacity;
    private String streamTimeout;
    private Map<String, String> outputMapping;
    private String connectTimeout;
    private String idleTimeout;
    private String queryVariables;
    private String queryPattern;
    private boolean failOnErrors;
    private String metricId;
    private boolean retainResponseType;

    public PgSourceConfigBuilder setHost(String host) {
        this.host = host;
        return this;
    }

    public PgSourceConfigBuilder setPort(String port) {
        this.port = port;
        return this;
    }

    public PgSourceConfigBuilder setUser(String user) {
        this.user = user;
        return this;
    }

    public PgSourceConfigBuilder setPassword(String password) {
        this.password = password;
        return this;
    }

    public PgSourceConfigBuilder setDatabase(String database) {
        this.database = database;
        return this;
    }

    public PgSourceConfigBuilder setType(String type) {
        this.type = type;
        return this;
    }

    public PgSourceConfigBuilder setCapacity(String capacity) {
        this.capacity = capacity;
        return this;
    }

    public PgSourceConfigBuilder setStreamTimeout(String streamTimeout) {
        this.streamTimeout = streamTimeout;
        return this;
    }

    public PgSourceConfigBuilder setOutputMapping(Map<String, String> outputMapping) {
        this.outputMapping = outputMapping;
        return this;
    }

    public PgSourceConfigBuilder setConnectTimeout(String connectTimeout) {
        this.connectTimeout = connectTimeout;
        return this;
    }

    public PgSourceConfigBuilder setIdleTimeout(String idleTimeout) {
        this.idleTimeout = idleTimeout;
        return this;
    }

    public PgSourceConfigBuilder setQueryVariables(String queryVariables) {
        this.queryVariables = queryVariables;
        return this;
    }

    public PgSourceConfigBuilder setQueryPattern(String queryPattern) {
        this.queryPattern = queryPattern;
        return this;
    }

    public PgSourceConfigBuilder setFailOnErrors(boolean failOnErrors) {
        this.failOnErrors = failOnErrors;
        return this;
    }

    public PgSourceConfigBuilder setMetricId(String metricId) {
        this.metricId = metricId;
        return this;
    }

    public PgSourceConfigBuilder setRetainResponseType(boolean retainResponseType) {
        this.retainResponseType = retainResponseType;
        return this;
    }

    public PgSourceConfig createPgSourceConfig() {
        return new PgSourceConfig(host, port, user, password, database, type, capacity, streamTimeout, outputMapping, connectTimeout, idleTimeout, queryVariables, queryPattern, failOnErrors, metricId, retainResponseType);
    }
}
