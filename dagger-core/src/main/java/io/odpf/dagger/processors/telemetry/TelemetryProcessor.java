package io.odpf.dagger.processors.telemetry;

import io.odpf.dagger.common.core.StreamInfo;
import io.odpf.dagger.processors.PostProcessorConfig;
import io.odpf.dagger.processors.types.PostProcessor;
import io.odpf.dagger.processors.telemetry.processor.MetricsTelemetryExporter;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

public class TelemetryProcessor implements PostProcessor {
    private MetricsTelemetryExporter metricsTelemetryExporter;

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
