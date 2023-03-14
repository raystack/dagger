package com.gotocompany.dagger.core.processors;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.gotocompany.dagger.core.processors.common.ValidRecordsDecorator;
import com.jayway.jsonpath.InvalidJsonException;
import com.gotocompany.dagger.common.configuration.Configuration;
import com.gotocompany.dagger.common.core.DaggerContext;
import com.gotocompany.dagger.common.core.StreamInfo;
import com.gotocompany.dagger.core.metrics.telemetry.TelemetryTypes;
import com.gotocompany.dagger.core.processors.telemetry.processor.MetricsTelemetryExporter;
import com.gotocompany.dagger.core.processors.transformers.TransformProcessor;
import com.gotocompany.dagger.core.processors.types.Preprocessor;
import com.gotocompany.dagger.core.utils.Constants;

import java.util.ArrayList;
import java.util.List;

/**
 * The Preprocessor orchestrator.
 */
public class PreProcessorOrchestrator implements Preprocessor {

    private final MetricsTelemetryExporter metricsTelemetryExporter;
    private final PreProcessorConfig processorConfig;
    private final String tableName;
    private final DaggerContext daggerContext;
    private static final Gson GSON = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();

    /**
     * Instantiates a new Preprocessor orchestrator.
     *
     * @param daggerContext            the daggerContext
     * @param metricsTelemetryExporter the metrics telemetry exporter
     * @param tableName                the table name
     */
    public PreProcessorOrchestrator(DaggerContext daggerContext, MetricsTelemetryExporter metricsTelemetryExporter, String tableName) {
        this.daggerContext = daggerContext;
        this.processorConfig = parseConfig(daggerContext.getConfiguration());
        this.metricsTelemetryExporter = metricsTelemetryExporter;
        this.tableName = tableName;
    }

    /**
     * Parse config preprocessor config.
     *
     * @param configuration the configuration
     * @return the preprocessor config
     */
    public PreProcessorConfig parseConfig(Configuration configuration) {
        if (!configuration.getBoolean(Constants.PROCESSOR_PREPROCESSOR_ENABLE_KEY, Constants.PROCESSOR_PREPROCESSOR_ENABLE_DEFAULT)) {
            return null;
        }
        String configJson = configuration.getString(Constants.PROCESSOR_PREPROCESSOR_CONFIG_KEY, "");
        PreProcessorConfig config;
        try {
            config = GSON.fromJson(configJson, PreProcessorConfig.class);
        } catch (JsonSyntaxException exception) {
            throw new InvalidJsonException("Invalid JSON Given for " + Constants.PROCESSOR_PREPROCESSOR_CONFIG_KEY);
        }
        return config;
    }

    @Override
    public StreamInfo process(StreamInfo streamInfo) {
        for (Preprocessor processor : getProcessors()) {
            streamInfo = processor.process(streamInfo);
        }
        return new StreamInfo(
                new ValidRecordsDecorator(tableName, streamInfo.getColumnNames(), daggerContext.getConfiguration())
                        .decorate(streamInfo.getDataStream()),
                streamInfo.getColumnNames());
    }

    /**
     * Gets processors.
     *
     * @return the processors
     */
    protected List<Preprocessor> getProcessors() {
        List<Preprocessor> preprocessors = new ArrayList<>();
        if (canProcess(processorConfig)) {
            processorConfig
                    .getTableTransformers()
                    .stream()
                    .filter(x -> x.getTableName().equals(this.tableName))
                    .forEach(elem -> {
                        TransformProcessor processor = new TransformProcessor(
                                elem.getTableName(),
                                TelemetryTypes.PRE_PROCESSOR_TYPE,
                                elem.getTransformers(),
                                daggerContext);
                        processor.notifySubscriber(metricsTelemetryExporter);
                        preprocessors.add(processor);
                    });
        }
        return preprocessors;
    }

    @Override
    public boolean canProcess(PreProcessorConfig config) {
        return config != null && !config.isEmpty();
    }

}
