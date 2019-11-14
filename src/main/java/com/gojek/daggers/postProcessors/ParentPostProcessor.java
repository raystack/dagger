package com.gojek.daggers.postProcessors;

import com.gojek.daggers.core.StreamInfo;
import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.daggers.postProcessors.common.PostProcessor;
import com.gojek.daggers.postProcessors.external.ExternalPostProcessor;
import com.gojek.daggers.postProcessors.external.common.FetchOutputDecorator;
import com.gojek.daggers.postProcessors.external.common.InitializationDecorator;
import com.gojek.daggers.postProcessors.internal.InternalPostProcessor;
import com.gojek.daggers.postProcessors.transfromers.TransformProcessor;
import com.gojek.de.stencil.StencilClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.gojek.daggers.utils.Constants.POST_PROCESSOR_ENABLED_KEY;
import static com.gojek.daggers.utils.Constants.POST_PROCESSOR_ENABLED_KEY_DEFAULT;

public class ParentPostProcessor implements PostProcessor {
    private final PostProcessorConfig postProcessorConfig;
    private final StencilClient stencilClient;
    private Configuration configuration;

    public ParentPostProcessor(PostProcessorConfig postProcessorConfig, Configuration configuration, StencilClient stencilClient) {
        this.postProcessorConfig = postProcessorConfig;
        this.configuration = configuration;
        this.stencilClient = stencilClient;
    }


    @Override
    public StreamInfo process(StreamInfo streamInfo) {
        if (!canProcess(postProcessorConfig)) return streamInfo;
        DataStream<Row> resultStream = streamInfo.getDataStream();
        ColumnNameManager columnNameManager = new ColumnNameManager(streamInfo.getColumnNames(), postProcessorConfig.getOutputColumnNames());

        InitializationDecorator initializationDecorator = new InitializationDecorator(columnNameManager);
        resultStream = initializationDecorator.decorate(resultStream);
        streamInfo = new StreamInfo(resultStream, streamInfo.getColumnNames());

        List<PostProcessor> enabledPostProcessors = getEnabledPostProcessors(stencilClient, columnNameManager);
        for (PostProcessor postProcessor : enabledPostProcessors)
            streamInfo = postProcessor.process(streamInfo);

        FetchOutputDecorator fetchOutputDecorator = new FetchOutputDecorator();
        resultStream = fetchOutputDecorator.decorate(streamInfo.getDataStream());
        StreamInfo resultantStreamInfo = new StreamInfo(resultStream, columnNameManager.getOutputColumnNames());
        TransformProcessor transformProcessor = new TransformProcessor(postProcessorConfig.getTransformers());
        if (transformProcessor.canProcess(postProcessorConfig))
            resultantStreamInfo = transformProcessor.process(resultantStreamInfo);

        return resultantStreamInfo;
    }

    public List<PostProcessor> getEnabledPostProcessors(StencilClient stencilClient, ColumnNameManager columnNameManager) {
        if (!configuration.getBoolean(POST_PROCESSOR_ENABLED_KEY, POST_PROCESSOR_ENABLED_KEY_DEFAULT))
            return new ArrayList<>();
        ArrayList<PostProcessor> postProcessors = new ArrayList<>();
        postProcessors.add(new ExternalPostProcessor(stencilClient, postProcessorConfig.getExternalSource(), columnNameManager));
        postProcessors.add(new InternalPostProcessor(postProcessorConfig));
        return postProcessors
                .stream()
                .filter(p -> p.canProcess(postProcessorConfig))
                .collect(Collectors.toList());
    }

    @Override
    public boolean canProcess(PostProcessorConfig postProcessorConfig) {
        return postProcessorConfig != null && !postProcessorConfig.isEmpty();
    }
}
