package io.odpf.dagger.processors;

import org.apache.flink.configuration.Configuration;

import io.odpf.dagger.common.core.StreamInfo;
import io.odpf.dagger.metrics.telemetry.TelemetryTypes;
import io.odpf.dagger.processors.common.ValidRecordsDecorator;
import io.odpf.dagger.processors.telemetry.processor.MetricsTelemetryExporter;
import io.odpf.dagger.processors.transformers.TransformProcessor;
import io.odpf.dagger.processors.types.Preprocessor;

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
