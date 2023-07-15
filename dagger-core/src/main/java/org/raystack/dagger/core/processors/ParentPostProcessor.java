package org.raystack.dagger.core.processors;

import org.raystack.dagger.common.core.DaggerContext;
import org.raystack.dagger.core.processors.common.FetchOutputDecorator;
import org.raystack.dagger.core.processors.common.InitializationDecorator;
import org.raystack.dagger.core.processors.common.SchemaConfig;
import org.raystack.dagger.core.processors.types.PostProcessor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import org.raystack.dagger.common.configuration.Configuration;
import org.raystack.dagger.common.core.StencilClientOrchestrator;
import org.raystack.dagger.common.core.StreamInfo;
import org.raystack.dagger.core.metrics.telemetry.TelemetrySubscriber;
import org.raystack.dagger.core.processors.external.ExternalMetricConfig;
import org.raystack.dagger.core.processors.external.ExternalPostProcessor;
import org.raystack.dagger.core.processors.internal.InternalPostProcessor;
import org.raystack.dagger.core.processors.transformers.TransformProcessor;
import org.raystack.dagger.core.utils.Constants;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The Parent post processor.
 */
public class ParentPostProcessor implements PostProcessor {
    private final PostProcessorConfig postProcessorConfig;

    private final StencilClientOrchestrator stencilClientOrchestrator;
    private TelemetrySubscriber telemetrySubscriber;

    private final DaggerContext daggerContext;

    /**
     * Instantiates a new Parent post processor.
     *
     * @param daggerContext             the daggerContext
     * @param stencilClientOrchestrator the stencil client orchestrator
     * @param telemetrySubscriber       the telemetry subscriber
     */
    public ParentPostProcessor(DaggerContext daggerContext, StencilClientOrchestrator stencilClientOrchestrator, TelemetrySubscriber telemetrySubscriber) {
        this.daggerContext = daggerContext;
        this.stencilClientOrchestrator = stencilClientOrchestrator;
        this.telemetrySubscriber = telemetrySubscriber;
        this.postProcessorConfig = parsePostProcessorConfig(daggerContext.getConfiguration());
    }

    private static PostProcessorConfig parsePostProcessorConfig(Configuration configuration) {
        String postProcessorConfigString = configuration.getString(Constants.PROCESSOR_POSTPROCESSOR_CONFIG_KEY, "");
        return PostProcessorConfig.parse(postProcessorConfigString);
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
        SchemaConfig schemaConfig = new SchemaConfig(daggerContext.getConfiguration(), stencilClientOrchestrator, columnNameManager);

        List<PostProcessor> enabledPostProcessors = getEnabledPostProcessors(telemetrySubscriber, schemaConfig);
        for (PostProcessor postProcessor : enabledPostProcessors) {
            streamInfo = postProcessor.process(streamInfo);
        }

        FetchOutputDecorator fetchOutputDecorator = new FetchOutputDecorator(schemaConfig, postProcessorConfig.hasSQLTransformer());
        resultStream = fetchOutputDecorator.decorate(streamInfo.getDataStream());
        StreamInfo resultantStreamInfo = new StreamInfo(resultStream, columnNameManager.getOutputColumnNames());
        TransformProcessor transformProcessor = new TransformProcessor(postProcessorConfig.getTransformers(), daggerContext);
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
        if (!daggerContext.getConfiguration().getBoolean(Constants.PROCESSOR_POSTPROCESSOR_ENABLE_KEY, Constants.PROCESSOR_POSTPROCESSOR_ENABLE_DEFAULT)) {
            return new ArrayList<>();
        }

        ExternalMetricConfig externalMetricConfig = getExternalMetricConfig(daggerContext.getConfiguration(), subscriber);
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
