package com.gojek.daggers.postprocessor;

import com.gojek.daggers.longbow.LongbowSchema;
import com.gojek.daggers.longbow.processor.LongbowReader;
import com.gojek.daggers.longbow.processor.LongbowWriter;
import com.gojek.daggers.longbow.row.LongbowRowFactory;
import com.gojek.daggers.postprocessor.parser.PostProcessorConfig;
import com.gojek.de.stencil.StencilClient;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.gojek.daggers.Constants.*;

public class PostProcessorFactory {

    public static List<PostProcessor> getPostProcessors(Configuration configuration, StencilClient stencilClient, String[] columnNames) {
        List<PostProcessor> postProcessors = new ArrayList<>();

        if (configuration.getBoolean(ASYNC_IO_ENABLED_KEY, ASYNC_IO_ENABLED_DEFAULT)) {
            postProcessors.add(new AshikoProcessorDeprecated(configuration, stencilClient));
        }
        if (configuration.getString(SQL_QUERY, SQL_QUERY_DEFAULT).contains(LONGBOW_KEY)) {
            postProcessors.add(getLongBowProcessor(columnNames, configuration));
        }
        postProcessors.addAll(getEnabledPostProcessors(configuration, stencilClient));
        return postProcessors;
    }

    private static List<PostProcessor> getEnabledPostProcessors(Configuration configuration, StencilClient stencilClient) {
        if(!configuration.getBoolean(POST_PROCESSOR_ENABLED_KEY, POST_PROCESSOR_ENABLED_KEY_DEFAULT))
            return new ArrayList<>();
        ArrayList<PostProcessor> postProcessors = new ArrayList<>();
        PostProcessorConfig postProcessorConfig = parsePostProcessorConfig(configuration);
        postProcessors.add(new ExternalSourceProcessor(configuration, stencilClient, postProcessorConfig));
        postProcessors.add(new TransformProcessor(postProcessorConfig.getTransformers()));
        return postProcessors
                .stream()
                .filter(p -> p.canProcess(configuration,postProcessorConfig))
                .collect(Collectors.toList());
    }

    private static LongbowProcessor getLongBowProcessor(String[] columnNames, Configuration configuration) {
        final LongbowSchema longbowSchema = new LongbowSchema(columnNames);
        LongbowReader longbowReader = new LongbowReader(configuration, longbowSchema, LongbowRowFactory.getLongbowRow(longbowSchema));
        LongbowWriter longbowWriter = new LongbowWriter(configuration, longbowSchema);

        return new LongbowProcessor(longbowWriter, longbowReader, new AsyncProcessor(), longbowSchema, configuration);
    }

    private static PostProcessorConfig parsePostProcessorConfig(Configuration configuration) {
        String postProcessorConfigString = configuration.getString(POST_PROCESSOR_CONFIG_KEY, "");
        return PostProcessorConfig.parse(postProcessorConfigString);
    }
}
