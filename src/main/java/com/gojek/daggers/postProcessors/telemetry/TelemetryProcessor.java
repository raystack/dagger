package com.gojek.daggers.postProcessors.telemetry;

import com.gojek.daggers.core.StreamInfo;
import com.gojek.daggers.postProcessors.PostProcessorConfig;
import com.gojek.daggers.postProcessors.common.PostProcessor;
import com.gojek.daggers.postProcessors.telemetry.processor.TelemetryExporter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

public class TelemetryProcessor implements PostProcessor {
    private TelemetryExporter telemetryExporter;

    public TelemetryProcessor(TelemetryExporter telemetryExporter) {
        this.telemetryExporter = telemetryExporter;
    }

    @Override
    public StreamInfo process(StreamInfo inputStreamInfo) {
        DataStream<Row> resultStream = inputStreamInfo.getDataStream().map(telemetryExporter);
        return new StreamInfo(resultStream, inputStreamInfo.getColumnNames());
    }

    @Override
    public boolean canProcess(PostProcessorConfig postProcessorConfig) {
        return true;
    }
}
