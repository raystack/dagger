package com.gojek.daggers.postprocessors.telemetry;

import com.gojek.dagger.common.StreamInfo;
import com.gojek.daggers.postprocessors.PostProcessorConfig;
import com.gojek.daggers.postprocessors.common.PostProcessor;
import com.gojek.daggers.postprocessors.telemetry.processor.MetricsTelemetryExporter;
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
