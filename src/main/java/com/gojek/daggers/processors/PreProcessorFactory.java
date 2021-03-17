package com.gojek.daggers.processors;

import com.gojek.daggers.processors.telemetry.processor.MetricsTelemetryExporter;
import com.gojek.daggers.processors.types.Preprocessor;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.jayway.jsonpath.InvalidJsonException;
import org.apache.flink.configuration.Configuration;

import java.util.Collections;
import java.util.List;

import static com.gojek.daggers.utils.Constants.PRE_PROCESSOR_CONFIG_KEY;

public class PreProcessorFactory {
    private static final Gson gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();

    public static PreProcessorConfig parseConfig(Configuration configuration) {
        String configJson = configuration.getString(PRE_PROCESSOR_CONFIG_KEY, "");
        PreProcessorConfig config;
        try {
            config = gson.fromJson(configJson, PreProcessorConfig.class);
        } catch (JsonSyntaxException exception) {
            throw new InvalidJsonException("Invalid JSON Given for " + PRE_PROCESSOR_CONFIG_KEY);
        }
        return config;

    }

    public static List<Preprocessor> getPreProcessors(Configuration configuration, PreProcessorConfig processorConfig, String tableName, MetricsTelemetryExporter metricsTelemetryExporter) {
        return Collections.singletonList(new PreProcessorOrchestrator(configuration, processorConfig, metricsTelemetryExporter, tableName));
    }
}
