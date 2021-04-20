package io.odpf.dagger.processors;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.contracts.TelemetrySubscriber;
import io.odpf.dagger.common.core.StreamInfo;
import io.odpf.dagger.core.StencilClientOrchestrator;
import io.odpf.dagger.processors.common.FetchOutputDecorator;
import io.odpf.dagger.processors.common.InitializationDecorator;
import io.odpf.dagger.processors.external.ExternalMetricConfig;
import io.odpf.dagger.processors.external.ExternalPostProcessor;
import io.odpf.dagger.processors.external.SchemaConfig;
import io.odpf.dagger.processors.internal.InternalPostProcessor;
import io.odpf.dagger.processors.transformers.TransformProcessor;
import io.odpf.dagger.processors.types.PostProcessor;
import io.odpf.dagger.utils.Constants;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ParentPostProcessor implements PostProcessor {
    private final PostProcessorConfig postProcessorConfig;
    private final StencilClientOrchestrator stencilClientOrchestrator;
    private TelemetrySubscriber telemetrySubscriber;
    private Configuration configuration;

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
    public boolean canProcess(PostProcessorConfig postProcessorConfig) {
        return postProcessorConfig != null && !postProcessorConfig.isEmpty();
    }

    private List<PostProcessor> getEnabledPostProcessors(TelemetrySubscriber telemetrySubscriber, SchemaConfig schemaConfig) {
        if (!configuration.getBoolean(Constants.POST_PROCESSOR_ENABLED_KEY, Constants.POST_PROCESSOR_ENABLED_KEY_DEFAULT)) {
            return new ArrayList<>();
        }

        ExternalMetricConfig externalMetricConfig = getExternalMetricConfig(configuration, telemetrySubscriber);
        ArrayList<PostProcessor> processors = new ArrayList<>();
        processors.add(new ExternalPostProcessor(schemaConfig, postProcessorConfig.getExternalSource(), externalMetricConfig));
        processors.add(new InternalPostProcessor(postProcessorConfig));
        return processors
                .stream()
                .filter(p -> p.canProcess(postProcessorConfig))
                .collect(Collectors.toList());
    }

    private ExternalMetricConfig getExternalMetricConfig(Configuration configuration, TelemetrySubscriber telemetrySubscriber) {
        return new ExternalMetricConfig(configuration, telemetrySubscriber);
    }

}
