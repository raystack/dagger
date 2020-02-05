package com.gojek.daggers.postProcessors;

import static com.gojek.daggers.utils.Constants.LONGBOW_KEY;
import static com.gojek.daggers.utils.Constants.LONGBOW_VERSION_DEFAULT;
import static com.gojek.daggers.utils.Constants.LONGBOW_VERSION_KEY;
import static com.gojek.daggers.utils.Constants.POST_PROCESSOR_CONFIG_KEY;
import static com.gojek.daggers.utils.Constants.POST_PROCESSOR_ENABLED_KEY;
import static com.gojek.daggers.utils.Constants.POST_PROCESSOR_ENABLED_KEY_DEFAULT;
import static com.gojek.daggers.utils.Constants.SQL_QUERY;
import static com.gojek.daggers.utils.Constants.SQL_QUERY_DEFAULT;
import static com.gojek.daggers.utils.Constants.TELEMETRY_ENABLED_KEY;
import static com.gojek.daggers.utils.Constants.TELEMETRY_ENABLED_VALUE_DEFAULT;

import java.util.ArrayList;
import java.util.List;

import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.daggers.postProcessors.common.AsyncProcessor;
import com.gojek.daggers.postProcessors.common.PostProcessor;
import com.gojek.daggers.postProcessors.longbow.LongbowProcessor;
import com.gojek.daggers.postProcessors.longbow.LongbowSchema;
import com.gojek.daggers.postProcessors.longbow.processor.LongbowReader;
import com.gojek.daggers.postProcessors.longbow.processor.LongbowWriter;
import com.gojek.daggers.postProcessors.longbow.processor.PutRequestFactory;
import com.gojek.daggers.postProcessors.longbow.row.LongbowRowFactory;
import com.gojek.daggers.postProcessors.telemetry.TelemetryProcessor;
import com.gojek.daggers.postProcessors.telemetry.processor.MetricsTelemetryExporter;

import org.apache.flink.configuration.Configuration;

public class PostProcessorFactory {

    public static List<PostProcessor> getPostProcessors(Configuration configuration, StencilClientOrchestrator stencilClientOrchestrator, String[] columnNames, MetricsTelemetryExporter metricsTelemetryExporter) {
        List<PostProcessor> postProcessors = new ArrayList<>();
        if (configuration.getString(SQL_QUERY, SQL_QUERY_DEFAULT).contains(LONGBOW_KEY))
            postProcessors.add(getLongBowProcessor(columnNames, configuration, metricsTelemetryExporter));
        if (configuration.getBoolean(POST_PROCESSOR_ENABLED_KEY, POST_PROCESSOR_ENABLED_KEY_DEFAULT))
            postProcessors.add(new ParentPostProcessor(parsePostProcessorConfig(configuration), configuration, stencilClientOrchestrator, metricsTelemetryExporter));
        if (configuration.getBoolean(TELEMETRY_ENABLED_KEY, TELEMETRY_ENABLED_VALUE_DEFAULT)) {
            postProcessors.add(new TelemetryProcessor(metricsTelemetryExporter));
        }
        return postProcessors;
    }

    private static LongbowProcessor getLongBowProcessor(String[] columnNames, Configuration configuration, MetricsTelemetryExporter metricsTelemetryExporter) {
        final LongbowSchema longbowSchema = new LongbowSchema(columnNames);

        LongbowReader longbowReader = new LongbowReader(configuration, longbowSchema, LongbowRowFactory.getLongbowRow(longbowSchema));
        LongbowWriter longbowWriter = getLongbowWriterFactory(configuration, longbowSchema);

        longbowWriter.notifySubscriber(metricsTelemetryExporter);
        longbowReader.notifySubscriber(metricsTelemetryExporter);

        return new LongbowProcessor(longbowWriter, longbowReader, new AsyncProcessor(), longbowSchema, configuration);
    }

    private static LongbowWriter getLongbowWriterFactory(Configuration configuration, LongbowSchema longbowSchema) {
        if (configuration.getString(LONGBOW_VERSION_KEY, LONGBOW_VERSION_DEFAULT).equals("1")) {
            return new LongbowWriter(configuration, longbowSchema, new PutRequestFactory(longbowSchema));
        } else return null;
    }

    private static PostProcessorConfig parsePostProcessorConfig(Configuration configuration) {
        String postProcessorConfigString = configuration.getString(POST_PROCESSOR_CONFIG_KEY, "");
        return PostProcessorConfig.parse(postProcessorConfigString);
    }
}
