package com.gojek.daggers.postProcessors.external.pg;

import com.gojek.daggers.postProcessors.external.common.SourceConfig;
import org.apache.commons.lang.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PgSourceConfig implements Serializable, SourceConfig {

    private final String host;
    private final String port;
    private final String user;
    private final String password;
    private final String database;
    private final String type;
    private final String capacity;
    private final String streamTimeout;
    private final Map<String, String> outputMapping;
    private final String connectTimeout;
    private final String idleTimeout;
    private final String queryVariables;
    private final String queryPattern;
    private boolean failOnErrors;
    private String metricId;

    public PgSourceConfig(String host, String port, String user, String password, String database,
                          String type, String capacity, String streamTimeout, Map<String, String> outputMapping, String connectTimeout, String idleTimeout, String queryVariables, String queryPattern, boolean failOnErrors, String metricId) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.database = database;
        this.type = type;
        this.capacity = capacity;
        this.outputMapping = outputMapping;
        this.connectTimeout = connectTimeout;
        this.idleTimeout = idleTimeout;
        this.streamTimeout = streamTimeout;
        this.queryVariables = queryVariables;
        this.queryPattern = queryPattern;
        this.failOnErrors = failOnErrors;
        this.metricId = metricId;
    }

    @Override
    public List<String> getOutputColumns() {
        return new ArrayList<>(outputMapping.keySet());
    }

    @Override
    public HashMap<String, Object> getMandatoryFields() {
        HashMap<String, Object> mandatoryFields = new HashMap<>();
        mandatoryFields.put("host", host);
        mandatoryFields.put("port", port);
        mandatoryFields.put("user", user);
        mandatoryFields.put("password", password);
        mandatoryFields.put("database", database);
        mandatoryFields.put("type", type);
        mandatoryFields.put("capacity", capacity);
        mandatoryFields.put("stream_timeout", streamTimeout);
        mandatoryFields.put("connect_timeout", connectTimeout);
        mandatoryFields.put("idle_timeout", idleTimeout);
        mandatoryFields.put("query_pattern", queryPattern);
        mandatoryFields.put("output_mapping", outputMapping);
        mandatoryFields.put("fail_on_errors", failOnErrors);

        return mandatoryFields;
    }

    public Integer getStreamTimeout() {
        return Integer.valueOf(streamTimeout);
    }

    public Integer getCapacity() {
        return Integer.valueOf(capacity);
    }

    public Integer getPort() {
        return Integer.valueOf(port);
    }

    public String getHost() {
        return host;
    }

    public String getDatabase() {
        return database;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public Integer getConnectTimeout() {
        return Integer.valueOf(connectTimeout);
    }

    public Integer getIdleTimeout() {
        return Integer.valueOf(idleTimeout);
    }

    public String getQueryVariables() {
        return queryVariables;
    }

    @Override
    public String getPattern() {
        return queryPattern;
    }

    @Override
    public String getVariables() {
        return queryVariables;
    }

    public boolean hasType() {
        return StringUtils.isNotEmpty(type);
    }

    public String getType() {
        return type;
    }

    public String getMappedQueryParam(String outputColumn) {
        return outputMapping.get(outputColumn);
    }

    public boolean isFailOnErrors() {
        return failOnErrors;
    }

    @Override
    public String getMetricId() {
        return metricId;
    }
}
