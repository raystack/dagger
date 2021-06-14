package io.odpf.dagger.core.metrics.telemetry;

/**
 * The enum Telemetry types.
 */
public enum TelemetryTypes {
    INPUT_TOPIC("input_topic"),
    INPUT_PROTO("input_proto"),
    INPUT_STREAM("input_stream"),
    SINK_TYPE("sink_type"),
    OUTPUT_TOPIC("output_topic"),
    OUTPUT_PROTO("output_proto"),
    OUTPUT_STREAM("output_stream"),
    POST_PROCESSOR_TYPE("post_processor_type"),
    PRE_PROCESSOR_TYPE("pre_processor_type"),
    SOURCE_METRIC_ID("source_metricId");

    /**
     * Gets telemetry type value.
     *
     * @return the value
     */
    public String getValue() {
        return value;
    }

    private String value;

    TelemetryTypes(String value) {
        this.value = value;
    }
}
