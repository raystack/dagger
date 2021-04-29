package io.odpf.dagger.core.processors.external.es;

import io.odpf.dagger.core.processors.common.OutputMapping;
import io.odpf.dagger.core.processors.types.SourceConfig;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EsSourceConfig implements Serializable, SourceConfig {
    private final String host;
    private final String port;
    private final String user;
    private final String password;
    private final String endpointPattern;
    private final String endpointVariables;
    @SerializedName(value = "type", alternate = {"Type", "TYPE"})
    private final String type;
    private final String capacity;
    private final String retryTimeout;
    private final String socketTimeout;
    private final String streamTimeout;
    private final String connectTimeout;
    private final boolean failOnErrors;
    private final Map<String, OutputMapping> outputMapping;
    @SerializedName(value = "metricId", alternate = {"MetricId", "METRICID"})
    private final String metricId;
    private final boolean retainResponseType;


    public EsSourceConfig(String host, String port, String user, String password, String endpointPattern, String endpointVariables,
                          String type, String capacity, String connectTimeout, String retryTimeout, String socketTimeout, String streamTimeout,
                          boolean failOnErrors, Map<String, OutputMapping> outputMapping, String metricId, boolean retainResponseType) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
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
        this.retainResponseType = retainResponseType;
    }


    public String getHost() {
        return host;
    }

    public Integer getPort() {
        return Integer.valueOf(port);
    }

    public String getUser() {
        return user == null ? "" : user;
    }

    public String getPassword() {
        return password == null ? "" : password;
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

    public boolean isRetainResponseType() {
        return retainResponseType;
    }
}
