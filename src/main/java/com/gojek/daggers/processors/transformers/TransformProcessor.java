package com.gojek.daggers.processors.transformers;

import org.apache.flink.configuration.Configuration;

import com.gojek.daggers.core.StreamInfo;
import com.gojek.daggers.metrics.telemetry.TelemetryPublisher;
import com.gojek.daggers.metrics.telemetry.TelemetryTypes;
import com.gojek.daggers.processors.PostProcessorConfig;
import com.gojek.daggers.processors.PreProcessorConfig;
import com.gojek.daggers.processors.types.PostProcessor;
import com.gojek.daggers.processors.types.Preprocessor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.gojek.daggers.utils.Constants.TRANSFORM_PROCESSOR;

public class TransformProcessor implements Preprocessor, PostProcessor, TelemetryPublisher {
    protected final List<TransformConfig> transformConfigs;

    public String getTableName() {
        return tableName;
    }

    protected final String tableName;
    private final Configuration configuration;
    private final Map<String, List<String>> metrics = new HashMap<>();
    protected final TelemetryTypes type;

    public TransformProcessor(List<TransformConfig> transformConfigs, Configuration configuration) {
        this("NULL", TelemetryTypes.POST_PROCESSOR_TYPE, transformConfigs, configuration);
    }

    public TransformProcessor(String tableName, TelemetryTypes type, List<TransformConfig> transformConfigs, Configuration configuration) {
        this.transformConfigs = transformConfigs == null ? new ArrayList<>() : transformConfigs;
        this.configuration = configuration;
        this.tableName = tableName;
        this.type = type;
        TransformerUtils.populateDefaultArguments(this);
    }

    //TODO : Removed Transformers references as part of internal dependencies removal. Will re-add them later
    @Override
    public StreamInfo process(StreamInfo streamInfo) {
        for (TransformConfig transformConfig : transformConfigs) {
            String className = transformConfig.getTransformationClass();
        }
        return streamInfo;
    }

    @Override
    public boolean canProcess(PreProcessorConfig processorConfig) {
        return processorConfig.getTableTransformers().stream().anyMatch(x -> x.tableName.equals(this.tableName));
    }

    @Override
    public boolean canProcess(PostProcessorConfig processorConfig) {
        return processorConfig.hasTransformConfigs();
    }

    @Override
    public void preProcessBeforeNotifyingSubscriber() {
        switch (this.type) {
            case POST_PROCESSOR_TYPE:
                addMetric(type.getValue(), TRANSFORM_PROCESSOR);
                break;
            case PRE_PROCESSOR_TYPE:
                addMetric(type.getValue(), this.tableName + "_" + TRANSFORM_PROCESSOR);
                break;
        }
    }


    @Override
    public Map<String, List<String>> getTelemetry() {
        return metrics;
    }

    private void addMetric(String key, String value) {
        metrics.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    }
}
