package io.odpf.dagger.core.processors.telemetry;

import io.odpf.dagger.common.core.StreamInfo;
import io.odpf.dagger.core.processors.PostProcessorConfig;
import io.odpf.dagger.core.processors.types.PostProcessor;
import io.odpf.dagger.core.processors.telemetry.processor.MetricsTelemetryExporter;

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
