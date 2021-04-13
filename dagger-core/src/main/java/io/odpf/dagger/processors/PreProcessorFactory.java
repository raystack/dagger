package io.odpf.dagger.processors;

import io.odpf.dagger.processors.telemetry.processor.MetricsTelemetryExporter;
import io.odpf.dagger.processors.types.Preprocessor;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.jayway.jsonpath.InvalidJsonException;
import io.odpf.dagger.utils.Constants;

import org.apache.flink.configuration.Configuration;

import java.util.Collections;
import java.util.List;

public class PreProcessorFactory {
    private static final Gson gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();

    public static PreProcessorConfig parseConfig(Configuration configuration) {
        if (!configuration.getBoolean(Constants.PRE_PROCESSOR_ENABLED_KEY, Constants.PRE_PROCESSOR_ENABLED_DEFAULT)) {
            return null;
        }
        String configJson = configuration.getString(Constants.PRE_PROCESSOR_CONFIG_KEY, "");
        PreProcessorConfig config;
        try {
            config = gson.fromJson(configJson, PreProcessorConfig.class);
        } catch (JsonSyntaxException exception) {
            throw new InvalidJsonException("Invalid JSON Given for " + Constants.PRE_PROCESSOR_CONFIG_KEY);
        }
        return config;
    }

    public static List<Preprocessor> getPreProcessors(Configuration configuration, PreProcessorConfig processorConfig, String tableName, MetricsTelemetryExporter metricsTelemetryExporter) {
        return Collections.singletonList(new PreProcessorOrchestrator(configuration, processorConfig, metricsTelemetryExporter, tableName));
    }
}
