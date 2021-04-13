package io.odpf.dagger.processors;

import io.odpf.dagger.core.StencilClientOrchestrator;
import io.odpf.dagger.processors.longbow.LongbowFactory;
import io.odpf.dagger.processors.longbow.LongbowSchema;
import io.odpf.dagger.processors.telemetry.TelemetryProcessor;
import io.odpf.dagger.processors.telemetry.processor.MetricsTelemetryExporter;
import io.odpf.dagger.processors.types.PostProcessor;
import io.odpf.dagger.utils.Constants;

import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class PostProcessorFactory {

    public static List<PostProcessor> getPostProcessors(Configuration configuration, StencilClientOrchestrator stencilClientOrchestrator, String[] columnNames, MetricsTelemetryExporter metricsTelemetryExporter) {
        List<PostProcessor> postProcessors = new ArrayList<>();

        if (Arrays.stream(columnNames).anyMatch(s -> Pattern.compile(".*\\blongbow.*key\\b.*").matcher(s).find()))
            postProcessors.add(getLongBowProcessor(columnNames, configuration, metricsTelemetryExporter, stencilClientOrchestrator));
        if (configuration.getBoolean(Constants.POST_PROCESSOR_ENABLED_KEY, Constants.POST_PROCESSOR_ENABLED_KEY_DEFAULT))
            postProcessors.add(new ParentPostProcessor(parsePostProcessorConfig(configuration), configuration, stencilClientOrchestrator, metricsTelemetryExporter));
        if (configuration.getBoolean(Constants.TELEMETRY_ENABLED_KEY, Constants.TELEMETRY_ENABLED_VALUE_DEFAULT)) {
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
        String postProcessorConfigString = configuration.getString(Constants.POST_PROCESSOR_CONFIG_KEY, "");
        return PostProcessorConfig.parse(postProcessorConfigString);
    }
}
