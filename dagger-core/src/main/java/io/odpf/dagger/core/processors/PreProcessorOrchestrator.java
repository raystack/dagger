package io.odpf.dagger.core.processors;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StreamInfo;
import io.odpf.dagger.core.metrics.telemetry.TelemetryTypes;
import io.odpf.dagger.core.processors.common.ValidRecordsDecorator;
import io.odpf.dagger.core.processors.telemetry.processor.MetricsTelemetryExporter;
import io.odpf.dagger.core.processors.transformers.TransformProcessor;
import io.odpf.dagger.core.processors.types.Preprocessor;

import java.util.ArrayList;
import java.util.List;

/**
 * The Preprocessor orchestrator.
 */
public class PreProcessorOrchestrator implements Preprocessor {

    private final MetricsTelemetryExporter metricsTelemetryExporter;
    private Configuration configuration;
    private final PreProcessorConfig processorConfig;
    private final String tableName;

    /**
     * Instantiates a new Preprocessor orchestrator.
     *
     * @param configuration            the configuration
     * @param processorConfig          the processor config
     * @param metricsTelemetryExporter the metrics telemetry exporter
     * @param tableName                the table name
     */
    public PreProcessorOrchestrator(Configuration configuration, PreProcessorConfig processorConfig, MetricsTelemetryExporter metricsTelemetryExporter, String tableName) {
        this.configuration = configuration;
        this.processorConfig = processorConfig;
        this.metricsTelemetryExporter = metricsTelemetryExporter;
        this.tableName = tableName;
    }

    @Override
    public StreamInfo process(StreamInfo streamInfo) {
        for (Preprocessor processor : getProcessors()) {
            streamInfo = processor.process(streamInfo);
        }
        return new StreamInfo(
                new ValidRecordsDecorator(tableName, streamInfo.getColumnNames(), configuration)
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
                                configuration);
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
