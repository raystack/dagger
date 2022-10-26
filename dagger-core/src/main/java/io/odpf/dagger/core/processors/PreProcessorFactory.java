package io.odpf.dagger.core.processors;

import io.odpf.dagger.common.core.DaggerContext;
import io.odpf.dagger.core.processors.telemetry.processor.MetricsTelemetryExporter;
import io.odpf.dagger.core.processors.types.Preprocessor;

import java.util.Collections;
import java.util.List;

/**
 * The factory class for Preprocessor.
 */
public class PreProcessorFactory {
    /**
     * Gets preprocessors.
     *
     * @param daggerContext            the daggerContext
     * @param tableName                the table name
     * @param metricsTelemetryExporter the metrics telemetry exporter
     * @return the preprocessors
     */
    public static List<Preprocessor> getPreProcessors(DaggerContext daggerContext, String tableName, MetricsTelemetryExporter metricsTelemetryExporter) {
        return Collections.singletonList(new PreProcessorOrchestrator(daggerContext, metricsTelemetryExporter, tableName));
    }
}
