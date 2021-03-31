package com.gojek.daggers.processors;

import com.gojek.daggers.processors.types.PostProcessor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import com.gojek.daggers.core.StreamInfo;
import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.daggers.metrics.telemetry.TelemetrySubscriber;
import com.gojek.daggers.processors.external.ExternalMetricConfig;
import com.gojek.daggers.processors.external.ExternalPostProcessor;
import com.gojek.daggers.processors.external.SchemaConfig;
import com.gojek.daggers.processors.common.FetchOutputDecorator;
import com.gojek.daggers.processors.common.InitializationDecorator;
import com.gojek.daggers.processors.internal.InternalPostProcessor;
import com.gojek.daggers.processors.transformers.TransformProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.gojek.daggers.utils.Constants.POST_PROCESSOR_ENABLED_KEY;
import static com.gojek.daggers.utils.Constants.POST_PROCESSOR_ENABLED_KEY_DEFAULT;

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
        if (!configuration.getBoolean(POST_PROCESSOR_ENABLED_KEY, POST_PROCESSOR_ENABLED_KEY_DEFAULT)) {
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
