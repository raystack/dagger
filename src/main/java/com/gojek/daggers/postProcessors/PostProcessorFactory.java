package com.gojek.daggers.postProcessors;

import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.daggers.postProcessors.common.PostProcessor;
import com.gojek.daggers.postProcessors.longbow.LongbowFactory;
import com.gojek.daggers.postProcessors.longbow.LongbowSchema;
import com.gojek.daggers.postProcessors.telemetry.TelemetryProcessor;
import com.gojek.daggers.postProcessors.telemetry.processor.MetricsTelemetryExporter;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.List;

import static com.gojek.daggers.utils.Constants.*;

public class PostProcessorFactory {

    public static List<PostProcessor> getPostProcessors(Configuration configuration, StencilClientOrchestrator stencilClientOrchestrator, String[] columnNames, MetricsTelemetryExporter metricsTelemetryExporter) {
        List<PostProcessor> postProcessors = new ArrayList<>();

        if (configuration.getString(SQL_QUERY, SQL_QUERY_DEFAULT).contains(LONGBOW_DAGGER_IDENTIFIER))
            postProcessors.add(getLongBowProcessor(columnNames, configuration, metricsTelemetryExporter, stencilClientOrchestrator));
        if (configuration.getBoolean(POST_PROCESSOR_ENABLED_KEY, POST_PROCESSOR_ENABLED_KEY_DEFAULT))
            postProcessors.add(new ParentPostProcessor(parsePostProcessorConfig(configuration), configuration, stencilClientOrchestrator, metricsTelemetryExporter));
        if (configuration.getBoolean(TELEMETRY_ENABLED_KEY, TELEMETRY_ENABLED_VALUE_DEFAULT)) {
            postProcessors.add(new TelemetryProcessor(metricsTelemetryExporter));
        }
        return postProcessors;
    }

    private static PostProcessor getLongBowProcessor(String[] columnNames, Configuration configuration, MetricsTelemetryExporter metricsTelemetryExporter, StencilClientOrchestrator stencilClientOrchestrator) {
        final LongbowSchema longbowSchema = new LongbowSchema(columnNames);
        LongbowFactory longbowFactory = new LongbowFactory(longbowSchema, configuration, stencilClientOrchestrator, metricsTelemetryExporter);

        return longbowFactory.getLongbowProcessor();
    }

    private static PostProcessorConfig parsePostProcessorConfig(Configuration configuration) {
        String postProcessorConfigString = configuration.getString(POST_PROCESSOR_CONFIG_KEY, "");
        return PostProcessorConfig.parse(postProcessorConfigString);
    }
}
