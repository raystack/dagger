package com.gojek.daggers.metrics.telemetry;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.functions.AggregateFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.gojek.daggers.utils.Constants.SQL_QUERY;
import static com.gojek.daggers.utils.Constants.SQL_QUERY_DEFAULT;

/**
 * This class only publishes metrics for UDFs of AggregatedFunction type
 * For ScalarFunction the metrics are sent directly from UDFs
 * For AggregatedFunction it can not be done due to this bug in flink
 * ISSUE : https://issues.apache.org/jira/browse/FLINK-15040
 */
public class AggregatedUDFTelemetryPublisher implements TelemetryPublisher {

    private Configuration configuration;
    private Map<String, AggregateFunction> aggregateFunctions;
    private Map<String, List<String>> metrics = new HashMap<>();

    public AggregatedUDFTelemetryPublisher(Configuration configuration, Map<String, AggregateFunction> aggregateFunctions) {
        this.configuration = configuration;
        this.aggregateFunctions = aggregateFunctions;
    }

    @Override
    public void preProcessBeforeNotifyingSubscriber() {
        String lowerCaseSQLQuery = configuration.getString(SQL_QUERY, SQL_QUERY_DEFAULT).toLowerCase();
        aggregateFunctions.keySet().forEach(aggregateFunctionName -> {
            if (lowerCaseSQLQuery.contains(aggregateFunctionName.toLowerCase())) {
                addMetric(TelemetryTypes.UDF.getValue(), aggregateFunctionName);
            }
        });
    }

    @Override
    public Map<String, List<String>> getTelemetry() {
        return metrics;
    }

    private void addMetric(String key, String value) {
        metrics.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    }
}
