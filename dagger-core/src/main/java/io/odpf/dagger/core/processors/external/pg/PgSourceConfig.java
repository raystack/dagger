package io.odpf.dagger.core.processors.external.pg;

import io.odpf.dagger.core.processors.types.SourceConfig;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A class that holds Postgre configuration.
 */
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
    @SerializedName(value = "metricId", alternate = {"MetricId", "METRICID"})
    private String metricId;
    private boolean retainResponseType;

    /**
     * Instantiates a new Postgre source config.
     *
     * @param host               the host
     * @param port               the port
     * @param user               the user
     * @param password           the password
     * @param database           the database
     * @param type               the type
     * @param capacity           the capacity
     * @param streamTimeout      the stream timeout
     * @param outputMapping      the output mapping
     * @param connectTimeout     the connect timeout
     * @param idleTimeout        the idle timeout
     * @param queryVariables     the query variables
     * @param queryPattern       the query pattern
     * @param failOnErrors       the fail on errors
     * @param metricId           the metric id
     * @param retainResponseType the retain response type
     */
    public PgSourceConfig(String host, String port, String user, String password, String database,
                          String type, String capacity, String streamTimeout, Map<String, String> outputMapping, String connectTimeout, String idleTimeout, String queryVariables, String queryPattern, boolean failOnErrors, String metricId, boolean retainResponseType) {
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
        this.retainResponseType = retainResponseType;
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
        mandatoryFields.put("capacity", capacity);
        mandatoryFields.put("stream_timeout", streamTimeout);
        mandatoryFields.put("connect_timeout", connectTimeout);
        mandatoryFields.put("idle_timeout", idleTimeout);
        mandatoryFields.put("query_pattern", queryPattern);
        mandatoryFields.put("output_mapping", outputMapping);
        mandatoryFields.put("fail_on_errors", failOnErrors);

        return mandatoryFields;
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
     * Gets capacity.
     *
     * @return the capacity
     */
    public Integer getCapacity() {
        return Integer.valueOf(capacity);
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
     * Gets host.
     *
     * @return the host
     */
    public String getHost() {
        return host;
    }

    /**
     * Gets database.
     *
     * @return the database
     */
    public String getDatabase() {
        return database;
    }

    /**
     * Gets user.
     *
     * @return the user
     */
    public String getUser() {
        return user;
    }

    /**
     * Gets password.
     *
     * @return the password
     */
    public String getPassword() {
        return password;
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
     * Gets idle timeout.
     *
     * @return the idle timeout
     */
    public Integer getIdleTimeout() {
        return Integer.valueOf(idleTimeout);
    }

    /**
     * Gets query variables.
     *
     * @return the query variables
     */
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

    /**
     * Check if type config is not empty.
     *
     * @return the boolean
     */
    public boolean hasType() {
        return StringUtils.isNotEmpty(type);
    }

    public String getType() {
        return type;
    }

    /**
     * Gets mapped query param.
     *
     * @param outputColumn the output column
     * @return the mapped query param
     */
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

    /**
     * Check if it is retain response type.
     *
     * @return the boolean
     */
    public boolean isRetainResponseType() {
        return retainResponseType;
    }
}
