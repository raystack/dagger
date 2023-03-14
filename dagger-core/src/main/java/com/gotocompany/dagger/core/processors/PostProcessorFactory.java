package com.gotocompany.dagger.core.processors;

import com.gotocompany.dagger.common.configuration.Configuration;
import com.gotocompany.dagger.common.core.DaggerContext;
import com.gotocompany.dagger.common.core.StencilClientOrchestrator;
import com.gotocompany.dagger.core.processors.longbow.LongbowFactory;
import com.gotocompany.dagger.core.processors.types.PostProcessor;
import com.gotocompany.dagger.core.processors.longbow.LongbowSchema;
import com.gotocompany.dagger.core.processors.telemetry.TelemetryProcessor;
import com.gotocompany.dagger.core.processors.telemetry.processor.MetricsTelemetryExporter;
import com.gotocompany.dagger.core.utils.Constants;

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
     * @param daggerContext             the daggerContext
     * @param stencilClientOrchestrator the stencil client orchestrator
     * @param columnNames               the column names
     * @param metricsTelemetryExporter  the metrics telemetry exporter
     * @return the post processors
     */
    public static List<PostProcessor> getPostProcessors(DaggerContext daggerContext, StencilClientOrchestrator stencilClientOrchestrator, String[] columnNames, MetricsTelemetryExporter metricsTelemetryExporter) {
        List<PostProcessor> postProcessors = new ArrayList<>();

        if (Arrays.stream(columnNames).anyMatch(s -> Pattern.compile(".*\\blongbow.*key\\b.*").matcher(s).find())) {
            postProcessors.add(getLongBowProcessor(columnNames, daggerContext.getConfiguration(), metricsTelemetryExporter, stencilClientOrchestrator));
        }
        if (daggerContext.getConfiguration().getBoolean(Constants.PROCESSOR_POSTPROCESSOR_ENABLE_KEY, Constants.PROCESSOR_POSTPROCESSOR_ENABLE_DEFAULT)) {
            postProcessors.add(new ParentPostProcessor(daggerContext, stencilClientOrchestrator, metricsTelemetryExporter));
        }
        if (daggerContext.getConfiguration().getBoolean(Constants.METRIC_TELEMETRY_ENABLE_KEY, Constants.METRIC_TELEMETRY_ENABLE_VALUE_DEFAULT)) {
            postProcessors.add(new TelemetryProcessor(metricsTelemetryExporter));
        }
        return postProcessors;
    }

    private static PostProcessor getLongBowProcessor(String[] columnNames, Configuration configuration, MetricsTelemetryExporter metricsTelemetryExporter, StencilClientOrchestrator stencilClientOrchestrator) {
        final LongbowSchema longbowSchema = new LongbowSchema(columnNames);
        LongbowFactory longbowFactory = new LongbowFactory(longbowSchema, configuration, stencilClientOrchestrator, metricsTelemetryExporter);

        return longbowFactory.getLongbowProcessor();
    }
}
