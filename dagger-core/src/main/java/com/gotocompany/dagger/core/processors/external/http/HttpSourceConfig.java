package com.gotocompany.dagger.core.processors.external.http;

import com.gotocompany.dagger.core.processors.common.OutputMapping;
import com.gotocompany.dagger.core.processors.types.SourceConfig;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A class that holds Http configuration.
 */
public class HttpSourceConfig implements Serializable, SourceConfig {
    private String endpoint;
    private String endpointVariables;
    private String verb;
    private String requestPattern;
    private String requestVariables;
    private String headerPattern;
    private String headerVariables;
    private String streamTimeout;
    private String connectTimeout;
    private boolean failOnErrors;
    private String excludeFailOnErrorsCodeRange;
    @SerializedName(value = "type", alternate = {"Type", "TYPE"})
    private String type;
    private String capacity;
    @SerializedName(value = "headers", alternate = {"Headers", "HEADERS"})
    private Map<String, String> headers;
    private Map<String, OutputMapping> outputMapping;
    @SerializedName(value = "metricId", alternate = {"MetricId", "METRICID"})
    private String metricId;
    private boolean retainResponseType;

    /**
     * Instantiates a new Http source config.
     *
     * @param endpoint           the endpoint
     * @param endpointVariables  the endpoint variables
     * @param verb               the verb
     * @param requestPattern     the request pattern
     * @param requestVariables   the request variables
     * @param headerPattern      the dynamic header pattern
     * @param headerVariables    the header variables
     * @param streamTimeout      the stream timeout
     * @param connectTimeout     the connect timeout
     * @param failOnErrors       the fail on errors
     * @param excludeFailOnErrorsCodeRange the exclude fail on errors code range
     * @param type               the type
     * @param capacity           the capacity
     * @param headers            the static headers
     * @param outputMapping      the output mapping
     * @param metricId           the metric id
     * @param retainResponseType the retain response type
     */
    public HttpSourceConfig(String endpoint, String endpointVariables, String verb, String requestPattern, String requestVariables, String headerPattern, String headerVariables, String streamTimeout, String connectTimeout, boolean failOnErrors, String excludeFailOnErrorsCodeRange,  String type, String capacity, Map<String, String> headers, Map<String, OutputMapping> outputMapping, String metricId, boolean retainResponseType) {
        this.endpoint = endpoint;
        this.endpointVariables = endpointVariables;
        this.verb = verb;
        this.requestPattern = requestPattern;
        this.requestVariables = requestVariables;
        this.headerPattern = headerPattern;
        this.headerVariables = headerVariables;
        this.streamTimeout = streamTimeout;
        this.connectTimeout = connectTimeout;
        this.failOnErrors = failOnErrors;
        this.excludeFailOnErrorsCodeRange = excludeFailOnErrorsCodeRange;
        this.type = type;
        this.capacity = capacity;
        this.headers = headers;
        this.outputMapping = outputMapping;
        this.metricId = metricId;
        this.retainResponseType = retainResponseType;
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

    /**
     * Gets endpoint variables.
     *
     * @return the endpointVariables
     */
    public String getEndpointVariables() {
        return endpointVariables;
    }


    /**
     * Gets verb.
     *
     * @return the verb
     */
    public String getVerb() {
        return verb;
    }

    /**
     * Gets request variables.
     *
     * @return the request variables
     */
    public String getRequestVariables() {
        return requestVariables;
    }

    /**
     * Gets header pattern.
     *
     * @return the header pattern
     */
    public String getHeaderPattern() {
        return headerPattern;
    }

    /**
     * Gets header Variable.
     *
     * @return the header Variable
     */
    public String getHeaderVariables() {
        return headerVariables;
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

    /**
     * Gets failOnErrorsCodeRange Variable.
     *
     * @return the failOnErrorsCodeRange Variable
     */
    public String getExcludeFailOnErrorsCodeRange() {
        return excludeFailOnErrorsCodeRange;
    }


    @Override
    public String getMetricId() {
        return metricId;
    }

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
        mandatoryFields.put("verb", verb);
        mandatoryFields.put("failOnErrors", failOnErrors);
        mandatoryFields.put("capacity", capacity);
        mandatoryFields.put("requestPattern", requestPattern);
        mandatoryFields.put("requestVariables", requestVariables);
        mandatoryFields.put("streamTimeout", streamTimeout);
        mandatoryFields.put("connectTimeout", connectTimeout);
        mandatoryFields.put("outputMapping", outputMapping);

        return mandatoryFields;
    }

    /**
     * Gets capacity.
     *
     * @return the capacity
     */
    public Integer getCapacity() {
        return Integer.parseInt(capacity);
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
        HttpSourceConfig that = (HttpSourceConfig) o;
        return failOnErrors == that.failOnErrors && excludeFailOnErrorsCodeRange == that.excludeFailOnErrorsCodeRange && retainResponseType == that.retainResponseType && Objects.equals(endpoint, that.endpoint) && Objects.equals(verb, that.verb) && Objects.equals(requestPattern, that.requestPattern) && Objects.equals(requestVariables, that.requestVariables) && Objects.equals(headerPattern, that.headerPattern) && Objects.equals(headerVariables, that.headerVariables) && Objects.equals(streamTimeout, that.streamTimeout) && Objects.equals(connectTimeout, that.connectTimeout) && Objects.equals(type, that.type) && Objects.equals(capacity, that.capacity) && Objects.equals(headers, that.headers) && Objects.equals(outputMapping, that.outputMapping) && Objects.equals(metricId, that.metricId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(endpoint, endpointVariables, verb, requestPattern, requestVariables, headerPattern, headerVariables, streamTimeout, connectTimeout, failOnErrors, excludeFailOnErrorsCodeRange, type, capacity, headers, outputMapping, metricId, retainResponseType);
    }
}
