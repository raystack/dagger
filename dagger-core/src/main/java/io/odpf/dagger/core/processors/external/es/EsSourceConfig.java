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
import java.util.Objects;

/**
 * A class that holds ElasticSearch configuration.
 */
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


    /**
     * Instantiates a new ElasticSearch source config.
     *
     * @param host               the host
     * @param port               the port
     * @param user               the user
     * @param password           the password
     * @param endpointPattern    the endpoint pattern
     * @param endpointVariables  the endpoint variables
     * @param type               the type
     * @param capacity           the capacity
     * @param connectTimeout     the connect timeout
     * @param retryTimeout       the retry timeout
     * @param socketTimeout      the socket timeout
     * @param streamTimeout      the stream timeout
     * @param failOnErrors       the fail on errors
     * @param outputMapping      the output mapping
     * @param metricId           the metric id
     * @param retainResponseType the retain response type
     */
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


    /**
     * Gets host.
     *
     * @return the host
     */
    public String getHost() {
        return host;
    }

    /**
     * Gets port.
     *
     * @return the port
     */
    public Integer getPort() {
        return Integer.valueOf(port);
    }

    /**
     * Gets user.
     *
     * @return the user
     */
    public String getUser() {
        return user == null ? "" : user;
    }

    /**
     * Gets password.
     *
     * @return the password
     */
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

    /**
     * Check if type config is not empty.
     *
     * @return the boolean
     */
    public boolean hasType() {
        return StringUtils.isNotEmpty(type);
    }

    /**
     * Gets capacity.
     *
     * @return the capacity
     */
    public Integer getCapacity() {
        return Integer.valueOf(capacity);
    }

    /**
     * Gets retry timeout.
     *
     * @return the retry timeout
     */
    public Integer getRetryTimeout() {
        return Integer.valueOf(retryTimeout);
    }

    /**
     * Gets socket timeout.
     *
     * @return the socket timeout
     */
    public Integer getSocketTimeout() {
        return Integer.valueOf(socketTimeout);
    }

    /**
     * Gets stream timeout.
     *
     * @return the stream timeout
     */
    public Integer getStreamTimeout() {
        return Integer.valueOf(streamTimeout);
    }

    /**
     * Gets connect timeout.
     *
     * @return the connect timeout
     */
    public Integer getConnectTimeout() {
        return Integer.valueOf(connectTimeout);
    }

    /**
     * Gets path.
     *
     * @param outputColumn the output column
     * @return the path
     */
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

    /**
     * Check if it is retain response type.
     *
     * @return the boolean
     */
    public boolean isRetainResponseType() {
        return retainResponseType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EsSourceConfig that = (EsSourceConfig) o;
        return failOnErrors == that.failOnErrors && retainResponseType == that.retainResponseType && Objects.equals(host, that.host) && Objects.equals(port, that.port) && Objects.equals(user, that.user) && Objects.equals(password, that.password) && Objects.equals(endpointPattern, that.endpointPattern) && Objects.equals(endpointVariables, that.endpointVariables) && Objects.equals(type, that.type) && Objects.equals(capacity, that.capacity) && Objects.equals(retryTimeout, that.retryTimeout) && Objects.equals(socketTimeout, that.socketTimeout) && Objects.equals(streamTimeout, that.streamTimeout) && Objects.equals(connectTimeout, that.connectTimeout) && Objects.equals(outputMapping, that.outputMapping) && Objects.equals(metricId, that.metricId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port, user, password, endpointPattern, endpointVariables, type, capacity, retryTimeout, socketTimeout, streamTimeout, connectTimeout, failOnErrors, outputMapping, metricId, retainResponseType);
    }
}
