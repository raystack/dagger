package com.gojek.daggers.postProcessors;

import com.gojek.daggers.postProcessors.common.AsyncProcessor;
import com.gojek.daggers.postProcessors.common.PostProcessor;
import com.gojek.daggers.postProcessors.longbow.LongbowProcessor;
import com.gojek.daggers.postProcessors.longbow.LongbowSchema;
import com.gojek.daggers.postProcessors.longbow.processor.LongbowReader;
import com.gojek.daggers.postProcessors.longbow.processor.LongbowWriter;
import com.gojek.daggers.postProcessors.longbow.row.LongbowRowFactory;
import com.gojek.daggers.postProcessors.telemetry.TelemetryProcessor;
import com.gojek.daggers.postProcessors.telemetry.processor.TelemetryExporter;
import com.gojek.de.stencil.StencilClient;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.List;

import static com.gojek.daggers.utils.Constants.*;

public class PostProcessorFactory {

    public static List<PostProcessor> getPostProcessors(Configuration configuration, StencilClient stencilClient, String[] columnNames, TelemetryExporter telemetryExporter) {
        List<PostProcessor> postProcessors = new ArrayList<>();

        if (configuration.getString(SQL_QUERY, SQL_QUERY_DEFAULT).contains(LONGBOW_KEY))
            postProcessors.add(getLongBowProcessor(columnNames, configuration));
        if (configuration.getBoolean(POST_PROCESSOR_ENABLED_KEY, POST_PROCESSOR_ENABLED_KEY_DEFAULT))
            postProcessors.add(new ParentPostProcessor(parsePostProcessorConfig(configuration), configuration, stencilClient));
        if (configuration.getBoolean(TELEMETRY_ENABLED_KEY, TELEMETRY_ENABLED_VALUE_DEFAULT))
            postProcessors.add(new TelemetryProcessor(telemetryExporter));
        return postProcessors;
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
