package io.odpf.dagger.core.processors;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.core.StreamInfo;
import io.odpf.dagger.core.metrics.telemetry.TelemetrySubscriber;
import io.odpf.dagger.core.processors.common.FetchOutputDecorator;
import io.odpf.dagger.core.processors.common.InitializationDecorator;
import io.odpf.dagger.core.processors.external.ExternalMetricConfig;
import io.odpf.dagger.core.processors.external.ExternalPostProcessor;
import io.odpf.dagger.core.processors.common.SchemaConfig;
import io.odpf.dagger.core.processors.internal.InternalPostProcessor;
import io.odpf.dagger.core.processors.transformers.TransformProcessor;
import io.odpf.dagger.core.processors.types.PostProcessor;
import io.odpf.dagger.core.utils.Constants;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The Parent post processor.
 */
public class ParentPostProcessor implements PostProcessor {
    private final PostProcessorConfig postProcessorConfig;
    private Configuration configuration;
    private final StencilClientOrchestrator stencilClientOrchestrator;
    private TelemetrySubscriber telemetrySubscriber;

    /**
     * Instantiates a new Parent post processor.
     *
     * @param postProcessorConfig       the post processor config
     * @param configuration             the configuration
     * @param stencilClientOrchestrator the stencil client orchestrator
     * @param telemetrySubscriber       the telemetry subscriber
     */
    public ParentPostProcessor(PostProcessorConfig postProcessorConfig, Configuration configuration, StencilClientOrchestrator stencilClientOrchestrator, TelemetrySubscriber telemetrySubscriber) {
        this.postProcessorConfig = postProcessorConfig;
        this.configuration = configuration;
        this.stencilClientOrchestrator = stencilClientOrchestrator;
        this.telemetrySubscriber = telemetrySubscriber;
    }

    @Override
    public StreamInfo process(StreamInfo streamInfo) {
        if (!canProcess(postProcessorConfig)) {
            return streamInfo;
        }
        DataStream<Row> resultStream = streamInfo.getDataStream();
        ColumnNameManager columnNameManager = new ColumnNameManager(streamInfo.getColumnNames(), postProcessorConfig.getOutputColumnNames());

        InitializationDecorator initializationDecorator = new InitializationDecorator(columnNameManager);
        resultStream = initializationDecorator.decorate(resultStream);
        streamInfo = new StreamInfo(resultStream, streamInfo.getColumnNames());
        SchemaConfig schemaConfig = new SchemaConfig(configuration, stencilClientOrchestrator, columnNameManager);

        List<PostProcessor> enabledPostProcessors = getEnabledPostProcessors(telemetrySubscriber, schemaConfig);
        for (PostProcessor postProcessor : enabledPostProcessors) {
            streamInfo = postProcessor.process(streamInfo);
        }

        FetchOutputDecorator fetchOutputDecorator = new FetchOutputDecorator(schemaConfig, postProcessorConfig.hasSQLTransformer());
        resultStream = fetchOutputDecorator.decorate(streamInfo.getDataStream());
        StreamInfo resultantStreamInfo = new StreamInfo(resultStream, columnNameManager.getOutputColumnNames());
        TransformProcessor transformProcessor = new TransformProcessor(postProcessorConfig.getTransformers(), configuration);
        if (transformProcessor.canProcess(postProcessorConfig)) {
            transformProcessor.notifySubscriber(telemetrySubscriber);
            resultantStreamInfo = transformProcessor.process(resultantStreamInfo);
        }
        return resultantStreamInfo;
    }

    @Override
    public boolean canProcess(PostProcessorConfig config) {
        return config != null && !config.isEmpty();
    }

    private List<PostProcessor> getEnabledPostProcessors(TelemetrySubscriber subscriber, SchemaConfig schemaConfig) {
        if (!configuration.getBoolean(Constants.PROCESSOR_POSTPROCESSOR_ENABLE_KEY, Constants.PROCESSOR_POSTPROCESSOR_ENABLE_DEFAULT)) {
            return new ArrayList<>();
        }

        ExternalMetricConfig externalMetricConfig = getExternalMetricConfig(configuration, subscriber);
        ArrayList<PostProcessor> processors = new ArrayList<>();
        processors.add(new ExternalPostProcessor(schemaConfig, postProcessorConfig.getExternalSource(), externalMetricConfig));
        processors.add(new InternalPostProcessor(postProcessorConfig, schemaConfig));
        return processors
                .stream()
                .filter(p -> p.canProcess(postProcessorConfig))
                .collect(Collectors.toList());
    }

    private ExternalMetricConfig getExternalMetricConfig(Configuration config, TelemetrySubscriber subscriber) {
        return new ExternalMetricConfig(config, subscriber);
    }
}
