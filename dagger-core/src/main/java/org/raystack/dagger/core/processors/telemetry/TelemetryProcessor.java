package org.raystack.dagger.core.processors.telemetry;

import org.raystack.dagger.common.core.StreamInfo;
import org.raystack.dagger.core.processors.PostProcessorConfig;
import org.raystack.dagger.core.processors.telemetry.processor.MetricsTelemetryExporter;
import org.raystack.dagger.core.processors.types.PostProcessor;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

/**
 * The Telemetry processor.
 */
public class TelemetryProcessor implements PostProcessor {
    private MetricsTelemetryExporter metricsTelemetryExporter;

    /**
     * Instantiates a new Telemetry processor.
     *
     * @param metricsTelemetryExporter the metrics telemetry exporter
     */
    public TelemetryProcessor(MetricsTelemetryExporter metricsTelemetryExporter) {
        this.metricsTelemetryExporter = metricsTelemetryExporter;
    }

    @Override
    public StreamInfo process(StreamInfo inputStreamInfo) {
        DataStream<Row> resultStream = inputStreamInfo.getDataStream().map(metricsTelemetryExporter);
        return new StreamInfo(resultStream, inputStreamInfo.getColumnNames());
    }

    @Override
    public boolean canProcess(PostProcessorConfig postProcessorConfig) {
        return true;
    }
}
