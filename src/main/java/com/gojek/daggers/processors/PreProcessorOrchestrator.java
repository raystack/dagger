package com.gojek.daggers.processors;

import com.gojek.dagger.common.StreamInfo;
import com.gojek.daggers.metrics.telemetry.TelemetryTypes;
import com.gojek.daggers.processors.common.ValidRecordsDecorator;
import com.gojek.daggers.processors.telemetry.processor.MetricsTelemetryExporter;
import com.gojek.daggers.processors.transformers.TransformProcessor;
import com.gojek.daggers.processors.types.Preprocessor;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.List;

public class PreProcessorOrchestrator implements Preprocessor {

    private final MetricsTelemetryExporter metricsTelemetryExporter;
    private final PreProcessorConfig processorConfig;
    private final String tableName;
    private final Configuration configuration;

    public PreProcessorOrchestrator(Configuration configuration, PreProcessorConfig processorConfig, MetricsTelemetryExporter metricsTelemetryExporter, String tableName) {
        this.processorConfig = processorConfig;
        this.metricsTelemetryExporter = metricsTelemetryExporter;
        this.tableName = tableName;
        this.configuration = configuration;
    }

    @Override
    public StreamInfo process(StreamInfo streamInfo) {
        for (Preprocessor processor : getProcessors()) {
            streamInfo = processor.process(streamInfo);
        }
        return new StreamInfo(
                new ValidRecordsDecorator(tableName, streamInfo.getColumnNames())
                        .decorate(streamInfo.getDataStream()),
                streamInfo.getColumnNames());
    }

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
                                configuration);
                        processor.notifySubscriber(metricsTelemetryExporter);
                        preprocessors.add(processor);
                    });
        }
        return preprocessors;
    }

    @Override
    public boolean canProcess(PreProcessorConfig processorConfig) {
        return processorConfig != null && !processorConfig.isEmpty();
    }

}
