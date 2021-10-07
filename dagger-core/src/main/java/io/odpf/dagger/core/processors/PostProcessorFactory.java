package io.odpf.dagger.core.processors;

import io.odpf.dagger.common.configuration.UserConfiguration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.core.processors.longbow.LongbowFactory;
import io.odpf.dagger.core.processors.longbow.LongbowSchema;
import io.odpf.dagger.core.processors.telemetry.TelemetryProcessor;
import io.odpf.dagger.core.processors.telemetry.processor.MetricsTelemetryExporter;
import io.odpf.dagger.core.processors.types.PostProcessor;
import io.odpf.dagger.core.utils.Constants;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * The factory class for Post processor.
 */
public class PostProcessorFactory {

    /**
     * Gets post processors.
     *
     * @param userConfiguration             the configuration
     * @param stencilClientOrchestrator the stencil client orchestrator
     * @param columnNames               the column names
     * @param metricsTelemetryExporter  the metrics telemetry exporter
     * @return the post processors
     */
    public static List<PostProcessor> getPostProcessors(UserConfiguration userConfiguration, StencilClientOrchestrator stencilClientOrchestrator, String[] columnNames, MetricsTelemetryExporter metricsTelemetryExporter) {
        List<PostProcessor> postProcessors = new ArrayList<>();

        if (Arrays.stream(columnNames).anyMatch(s -> Pattern.compile(".*\\blongbow.*key\\b.*").matcher(s).find())) {
            postProcessors.add(getLongBowProcessor(columnNames, userConfiguration, metricsTelemetryExporter, stencilClientOrchestrator));
        }
        if (userConfiguration.getParam().getBoolean(Constants.PROCESSOR_POSTPROCESSOR_ENABLE_KEY, Constants.PROCESSOR_POSTPROCESSOR_ENABLE_DEFAULT)) {
            postProcessors.add(new ParentPostProcessor(parsePostProcessorConfig(userConfiguration), userConfiguration, stencilClientOrchestrator, metricsTelemetryExporter));
        }
        if (userConfiguration.getParam().getBoolean(Constants.METRIC_TELEMETRY_ENABLE_KEY, Constants.METRIC_TELEMETRY_ENABLE_VALUE_DEFAULT)) {
            postProcessors.add(new TelemetryProcessor(metricsTelemetryExporter));
        }
        return postProcessors;
    }

    private static PostProcessor getLongBowProcessor(String[] columnNames, UserConfiguration userConfiguration, MetricsTelemetryExporter metricsTelemetryExporter, StencilClientOrchestrator stencilClientOrchestrator) {
        final LongbowSchema longbowSchema = new LongbowSchema(columnNames);
        LongbowFactory longbowFactory = new LongbowFactory(longbowSchema, userConfiguration, stencilClientOrchestrator, metricsTelemetryExporter);

        return longbowFactory.getLongbowProcessor();
    }

    private static PostProcessorConfig parsePostProcessorConfig(UserConfiguration userConfiguration) {
        String postProcessorConfigString = userConfiguration.getParam().get(Constants.PROCESSOR_POSTPROCESSOR_CONFIG_KEY, "");
        return PostProcessorConfig.parse(postProcessorConfigString);
    }
}
